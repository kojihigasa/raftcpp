#include "raft.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <iomanip>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

RaftServer::RaftServer(
    State initialState,
    StateMachine& stateMachine,
    RaftTransport& transport,
    std::vector<ServerId> peerIds,
    RaftConfig config
)
    : state_(std::move(initialState)),
      stateMachine_(stateMachine),
      transport_(transport),
      peerIds_(std::move(peerIds)),
      config_(config),
      serverId_(generateUniqueServerId()),
      lastEnqueued_(state_.lastApplied) {
    if (config_.minElectionTimeout <= std::chrono::milliseconds::zero() ||
        config_.minElectionTimeout > config_.maxElectionTimeout) {
        throw std::invalid_argument("RaftConfig election timeout range is invalid");
    }

    electionTimeout_ = getRandomElectionTimeout();
    // Initialize at the body of the constructor to ensure that the applyThread_ is started after all member variables are initialized.
    applyThread_ = std::thread(&RaftServer::applyLoop, this);
    electionTimeoutThread_ = std::thread(&RaftServer::electionTimeoutLoop, this);
}

RaftServer::~RaftServer() {
    terminating_.store(true);
    // Note: Practically, notify_one() is sufficient because there is only one thread waiting on each condition variable,
    //       but, notify_all() is easier to understand that all waiting threads should be notified to terminate,
    //       and it is safer in case of future changes that introduce multiple waiting threads.
    applyCv_.notify_all();
    electionCv_.notify_all();

    if (applyThread_.joinable()) {
        applyThread_.join();
    }

    if (electionTimeoutThread_.joinable()) {
        electionTimeoutThread_.join();
    }
}

// Generate a unique server ID using a UUIDv7 string.
ServerId RaftServer::generateUniqueServerId() {
    std::array<unsigned char, 16> bytes{};

    // Get the current timestamp in milliseconds since the Unix epoch
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto timestamp = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());

    // Fill the first 6 bytes with the timestamp (48 bits)
    for (int i = 5; i >= 0; i--) {
        bytes[i] = static_cast<unsigned char>(timestamp >> (8 * (5 - i)));
    }

    // Fill the remaining 10 bytes with random values
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<unsigned int> distribution(0, 255);

    for (std::size_t i = 6; i < bytes.size(); i++) {
        bytes[i] = static_cast<unsigned char>(distribution(generator));
    }

    // Set the version and variant bits according to the UUIDv7 specification
    bytes[6] = static_cast<unsigned char>(bytes[6] & 0x0f) | 0x70; // Set the version to 7
    bytes[8] = static_cast<unsigned char>(bytes[8] & 0x3f) | 0x80; // Set the variant to 10

    // Convert the byte array to a UUID string representation
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    for (std::size_t i = 0; i < bytes.size(); i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            oss << '-';
        }
        oss << std::setw(2) << static_cast<unsigned int>(bytes[i]);
    }

    return oss.str();
}

RequestVoteResults RaftServer::requestVote(const RequestVoteArgs& args) {
    RequestVoteResults results;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);

        results.term = state_.currentTerm;

        if (args.term < state_.currentTerm) {
            results.voteGranted = false;
            return results;
        } else if (args.term > state_.currentTerm) {
            const bool wasLeader = raftState_ == RaftState::Leader;

            state_.currentTerm = args.term;
            results.term = state_.currentTerm;
            // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
            state_.votedFor.reset();
            // If the candidate's term is greater than the current term, the server should transition to the follower state.
            // Note: RequestVote RPCs are send to all servers, so they may be received by a server that is currently a candidate
            // or a leader who looks being unable to communicate with a majority of the servers in the cluster.
            raftState_ = RaftState::Follower;

            // As aforementioned, RequestVote RPC may be sent to a leader, so we need to reset timer in that case.
            if (wasLeader) {
                resetElectionTimer();
            }
        }

        // Check if the candidate's log is at least as up-to-date as the receiver's log
        // A log is considered up-to-date if its last log term is greater than the receiver's last log term,
        // or if the last log terms are equal, the candidate's last log index is greater than or equal to the receiver's last log index.
        bool logUpToDate = (args.lastLogTerm > (state_.log.empty() ? 0 : state_.log.back().term)) ||
                           (args.lastLogTerm == (state_.log.empty() ? 0 : state_.log.back().term) &&
                            args.lastLogIndex >= static_cast<std::uint64_t>(state_.log.size()));

        if ((!state_.votedFor.has_value() || state_.votedFor.value() == args.candidateId) && logUpToDate) {
            state_.votedFor = args.candidateId;
            results.voteGranted = true;
            // A granted vote counts as activity during the election timeout.
            // Reset the timer so this follower does not begin another election immediately.
            resetElectionTimer();
        } else {
            results.voteGranted = false;
        }
    }

    return results;
}

AppendEntriesResults RaftServer::appendEntries(const AppendEntriesArgs& args) {
    AppendEntriesResults results;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);

        results.term = state_.currentTerm;

        if (args.term < state_.currentTerm) {
            // Return early if the leader's term is less than the current term, indicating that the leader is outdated and the server should not accept its log entries.
            // Note: Not resetting the election timer as it allows the server to start an election sooner if the leader is outdated and not sending heartbeats.
            results.success = false;
            return results;
        } else if (args.term > state_.currentTerm) {
            // If the leader's term is greater than the current term, the server should transition to the follower state.
            // Note: AppendEntries RPCs are sent to all servers, so they may be received by a server that is currently a "leader",
            // e.g., who belonged to a small partition of the cluster and joined the whole cluster again after network partition recovered,
            // or a candidate waiting for votes while the leader comes reachable again after an intermittent issue.
            state_.currentTerm = args.term;
            results.term = state_.currentTerm;
            // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
            state_.votedFor.reset();
            raftState_ = RaftState::Follower;
        } else if (raftState_ == RaftState::Candidate) {
            // args.term == state_.currentTerm
            // If the server is a candidate, it should transition to the follower state when receiving AppendEntries RPCs from a valid leader with the same term.
            // e.g., 3 servers in the cluster, server A and server B come to Candidate at the same time,
            // then, server A becomes a leader after being voted by server C, server B is still a candidate, and server C is a follower.
            raftState_ = RaftState::Follower;
        }

        // Reset the election timer when receiving AppendEntries RPCs from the leader with the valid term
        // to prevent unnecessary elections, even if the log entries are not appended due to log inconsistency.
        resetElectionTimer();

        const std::size_t previousLogIndex = static_cast<std::size_t>(args.prevLogIndex);

        // Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
        // Raft maintains the following property to ensure the consistency of logs on different servers:
        // If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.
        if (previousLogIndex > 0 &&
            (previousLogIndex > state_.log.size() ||
             state_.log[previousLogIndex - 1].term != args.prevLogTerm)) {
            results.success = false;
            return results;
        }

        // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        // e.g., in case that the previous leader fails after replicating an entry to only some servers,
        // and a different server, which doesn't have the entry, is elected as the new leader. 
        for (std::size_t i = 0; i < args.entries.size(); i++) {
            const std::size_t logIndex = previousLogIndex + i;
            if (logIndex >= state_.log.size()) {
                break;
            }

            if (state_.log[logIndex].term != args.entries[i].term) {
                state_.log.resize(logIndex);
                break;
            }
        }

        // Keep matching entries and append any new entries not already in the log
        const std::size_t firstIndexToAppend = state_.log.size() - previousLogIndex;
        for (std::size_t i = firstIndexToAppend; i < args.entries.size(); i++) {
            state_.log.push_back(args.entries[i]);
        }

        if (args.leaderCommit > state_.commitIndex) {
            state_.commitIndex = std::min(args.leaderCommit, static_cast<std::uint64_t>(state_.log.size()));
        }
    }

    // Enqueue the newly committed entries to be applied to the state machine asynchronously
    // and get back to the leader immediately without waiting for the apply to complete.
    enqueueCommittedEntries();

    results.success = true;
    return results;
}

void RaftServer::applyLoop() {
    // Note: Draining the applyQueue_ even when terminating_ is set to true to ensure
    // that all committed log entries are applied to the state machine before the RaftServer is destroyed.
    while (true) {
        std::unique_lock<std::mutex> lock(applyMutex_);
        applyCv_.wait(lock, [this] {
            return terminating_ || (!applyQueue_.empty() && !applyFailed_.has_value());
        });

        if (terminating_ && applyQueue_.empty()) {
            break;
        }

        auto [index, entry] = applyQueue_.front();
        applyQueue_.pop();
        lock.unlock();

        // Apply the log entry to the state machine
        bool applied = stateMachine_.applyLogEntry(entry);
        if (!applied) {
            std::lock_guard<std::mutex> lock(applyMutex_);
            applyFailed_ = index;
            break;
        }

        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            state_.lastApplied = index;
        }
    }
}

void RaftServer::enqueueCommittedEntries() {
    {
        std::lock_guard<std::mutex> lock(applyMutex_);
        std::lock_guard<std::mutex> stateLock(stateMutex_);
        while (lastEnqueued_ < state_.commitIndex) {
            const std::uint64_t index = lastEnqueued_ + 1;
            applyQueue_.push({index, state_.log[static_cast<std::size_t>(index - 1)]});
            lastEnqueued_ = index;
        }
    }

    applyCv_.notify_one();
}

void RaftServer::electionTimeoutLoop() {
    std::unique_lock<std::mutex> lock(electionMutex_);
    while (!terminating_) {
        if (!electionTimerEnabled_) {
            electionCv_.wait(lock, [this] {
                return terminating_ || electionTimerEnabled_;
            });
            continue;
        }

        const std::uint64_t currentGeneration = timerGeneration_;
        const bool interrupted = electionCv_.wait_until(
            lock, std::chrono::steady_clock::now() + electionTimeout_, [this, currentGeneration] {
                return terminating_ || !electionTimerEnabled_ || timerGeneration_ != currentGeneration;
            }
        );

        if (terminating_) {
            break;
        }

        if (interrupted) {
            // The election timer was reset, so we should continue waiting for the next timeout
            continue;
        } else {
            // Reset the election timeout for the next election
            electionTimeout_ = getRandomElectionTimeout();
            const std::uint64_t electionTimerGeneration = ++timerGeneration_;

            // Unlock the mutex before starting the election to allow AppendEntries RPCs to reset the election timer while the election is in progress
            lock.unlock();
            startElection(electionTimerGeneration);
            lock.lock();
        }
    }
}

std::chrono::milliseconds RaftServer::getRandomElectionTimeout() const {
    static thread_local std::mt19937 generator(std::random_device{}());
    using Rep = std::chrono::milliseconds::rep;
    std::uniform_int_distribution<Rep> distribution(
        config_.minElectionTimeout.count(), config_.maxElectionTimeout.count()
    );
    return std::chrono::milliseconds(distribution(generator));
}

void RaftServer::resetElectionTimer() {
    {
        std::lock_guard<std::mutex> lock(electionMutex_);
        electionTimeout_ = getRandomElectionTimeout();
        electionTimerEnabled_ = true;
        ++timerGeneration_;
    }
    electionCv_.notify_one();
}

void RaftServer::stopElectionTimer() {
    {
        std::lock_guard<std::mutex> lock(electionMutex_);
        electionTimerEnabled_ = false;
        ++timerGeneration_;
    }
    electionCv_.notify_one();
}

void RaftServer::startElection(std::uint64_t expectedTimerGeneration) {
    {
        std::lock_guard<std::mutex> lock(electionMutex_);
        // If the election timer has been reset since this election started, we should not start a new election
        // e.g. in the case that the server received an AppendEntries RPC from a valid leader just before this election started.
        if (expectedTimerGeneration != timerGeneration_ || terminating_) {
            return;
        }
    }

    RequestVoteArgs args;
    std::uint64_t electionTerm;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);

        // A vote response may elect this server after the timeout expires but before this election starts.
        // Do not let the timeout handler demote a server that has already become leader.
        if (raftState_ == RaftState::Leader) {
            return;
        }

        raftState_ = RaftState::Candidate;
        electionTerm = ++state_.currentTerm;
        state_.votedFor = serverId_;

        args.term = electionTerm;
        args.candidateId = serverId_;
        args.lastLogIndex = static_cast<std::uint64_t>(state_.log.size());
        args.lastLogTerm = state_.log.empty() ? 0 : state_.log.back().term;
    }

    // Note: peerIds_.size() + 1 is used to include the server itself in the majority calculation
    const std::size_t majority = (peerIds_.size() + 1) / 2 + 1;
    // Use a shared pointer to a set to track the peers that have granted their votes to avoid double counting in case of duplicate responses
    auto grantedPeers = std::make_shared<std::set<ServerId>>();
    grantedPeers->insert(serverId_); // Count self-vote

    for (const auto& peerId : peerIds_) {
        transport_.sendRequestVote(peerId, args,
            [this, peerId, electionTerm, majority, grantedPeers](RequestVoteResults results) {
            std::lock_guard<std::mutex> lock(stateMutex_);

            // If the term in the response is greater than the current term, update the current term and transition to follower
            if (results.term > state_.currentTerm) {
                const bool wasLeader = raftState_ == RaftState::Leader;

                state_.currentTerm = results.term;
                state_.votedFor.reset();
                raftState_ = RaftState::Follower;

                // The candidate may already promote to the leader with majority of votes before completing callback of a peer with higher term.
                // So, we need to reset timer in that case because it has stopped.
                if (wasLeader) {
                    resetElectionTimer();
                }
                return;
            }

            // Ignore vote results that do not belong to the current election
            // Note: must check both the raftState_ and the currentTerm to ensure that the server is still a candidate.
            // e.g., the server may have transitioned to a follower state after receiving an AppendEntries RPC during this election
            // from a valid leader with the same term before callback execution with locking the stateMutex_.
            if (state_.currentTerm != electionTerm || raftState_ != RaftState::Candidate) {
                return;
            }

            if (results.term == electionTerm && results.voteGranted) {
                grantedPeers->insert(peerId);
                if (grantedPeers->size() >= majority) {
                    raftState_ = RaftState::Leader;
                    state_.initializeLeaderState(peerIds_.size() + 1); // +1 for self
                    stopElectionTimer();
                }
            }
        });
    }
}
