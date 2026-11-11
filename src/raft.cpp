#include "raft.hpp"
#include <algorithm>
#include <array>
#include <iomanip>
#include <random>
#include <sstream>

RaftServer::RaftServer(State& state, StateMachine& stateMachine)
    : state_(state),
      stateMachine_(stateMachine),
      serverId_(generateUniqueServerId()),
      lastEnqueued_(state.lastApplied) {
    // Initialize at the body of the constructor to ensure that the applyThread_ is started after all member variables are initialized.
    applyThread_ = std::thread(&RaftServer::applyLoop, this);
    electionTimeoutThread_ = std::thread(&RaftServer::electionTimeoutLoop, this);
}

RaftServer::~RaftServer() {
    {
        std::lock_guard<std::mutex> lock(applyMutex_);
        terminating_ = true;
    }

    applyCv_.notify_one();

    if (applyThread_.joinable()) {
        applyThread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(electionMutex_);
        terminating_ = true;
    }

    electionCv_.notify_one();

    if (electionTimeoutThread_.joinable()) {
        electionTimeoutThread_.join();
    }
}

// Generate a unique server ID using a UUIDv7 string.
std::string RaftServer::generateUniqueServerId() {
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

RequestVoteResults RaftServer::handleRequestVote(const RequestVoteArgs& args) {
    if (args.term > state_.currentTerm) {
        state_.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state_.votedFor.reset();
        // If the candidate's term is greater than the current term, the server should transition to the follower state.
        // Note: RequestVote RPCs are send to all servers, so they may be received by a server that is currently a candidate
        // or a leader who looks being unable to communicate with a majority of the servers in the cluster.
        raftState_ = RaftState::Follower;
    }

    RequestVoteResults results;
    results.term = state_.currentTerm;

    if (args.term < state_.currentTerm) {
        results.voteGranted = false;
        return results;
    }

    // Check if the candidate's log is at least as up-to-date as the receiver's log
    // A log is considered up-to-date if its last log term is greater than the receiver's last log term,
    // or if the last log terms are equal, the candidate's last log index is greater than or equal to the receiver's last log index.
    bool logUpToDate = (args.lastLogTerm > (state_.log.empty() ? 0 : state_.log.back().term)) ||
                       (args.lastLogTerm == (state_.log.empty() ? 0 : state_.log.back().term) &&
                        args.lastLogIndex >= static_cast<std::uint32_t>(state_.log.size()));

    if ((!state_.votedFor.has_value() || state_.votedFor.value() == args.candidateId) && logUpToDate) {
        state_.votedFor = args.candidateId;
        results.voteGranted = true;
    } else {
        results.voteGranted = false;
    }

    return results;
}

AppendEntriesResults RaftServer::handleAppendEntries(const AppendEntriesArgs& args) {
    if (args.term > state_.currentTerm) {
        state_.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state_.votedFor.reset();
        // If the leader's term is greater than the current term, the server should transition to the follower state.
        // Note: AppendEntries RPCs are sent to all servers, so they may be received by a server that is currently a "leader",
        // e.g., who belonged to a small partition of the cluster and joined the whole cluster again after network partition recovered,
        // or a candidate waiting for votes while the leader comes reachable again after an intermittent issue.
        raftState_ = RaftState::Follower;
    }

    AppendEntriesResults results;
    results.term = state_.currentTerm;

    if (args.term < state_.currentTerm) {
        results.success = false;
        return results;
    }

    // Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
    // Raft maintains the following property to ensure the consistency of logs on different servers:
    // If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.
    if (args.prevLogIndex > 0 && (args.prevLogIndex > static_cast<std::uint32_t>(state_.log.size()) ||
                                  state_.log[args.prevLogIndex - 1].term != args.prevLogTerm)) {
        results.success = false;
        return results;
    }

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    for (std::size_t i = 0; i < args.entries.size(); i++) {
        std::uint32_t logIndex = args.prevLogIndex + 1 + static_cast<std::uint32_t>(i);
        if (logIndex <= static_cast<std::uint32_t>(state_.log.size())) {
            if (state_.log[logIndex - 1].term != args.entries[i].term) {
                state_.log.resize(logIndex - 1);
                break;
            }
        }
    }

    // Append any new entries not already in the log
    for (const auto& entry : args.entries) {
        state_.log.push_back(entry);
    }

    if (args.leaderCommit > state_.commitIndex) {
        state_.commitIndex = std::min(args.leaderCommit, static_cast<std::uint32_t>(state_.log.size()));
    }

    // Enqueue the newly committed entries to be applied to the state machine asynchronously
    // and get back to the leader immediately without waiting for the apply to complete.
    enqueueCommittedEntries();

    {
        std::lock_guard<std::mutex> lock(electionMutex_);
        // Reset the election timer when receiving AppendEntries RPCs from the leader to prevent unnecessary elections
        electionTimeout_ = getRandomElectionTimeout();
        timerGeneration_++;
    }
    electionCv_.notify_one();

    results.success = true;
    return results;
}

void RaftServer::applyLoop() {
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

        state_.lastApplied = index;
    }
}

void RaftServer::enqueueCommittedEntries() {
    {
        std::lock_guard<std::mutex> lock(applyMutex_);
        while (lastEnqueued_ < state_.commitIndex) {
            const std::uint32_t index = lastEnqueued_ + 1;
            applyQueue_.push({index, state_.log[index - 1]});
            lastEnqueued_ = index;
        }
    }

    applyCv_.notify_one();
}

void RaftServer::electionTimeoutLoop() {
    std::unique_lock<std::mutex> lock(electionMutex_);
    while (true) {
        const std::uint64_t currentGeneration = timerGeneration_;
        const bool interrupted = electionCv_.wait_until(
            lock, std::chrono::steady_clock::now() + electionTimeout_, [this, currentGeneration] {
                return timerGeneration_ != currentGeneration;
            }
        );

        if (terminating_) {
            break;
        }

        if (interrupted) {
            // The election timer was reset, so we should continue waiting for the next timeout
            continue;
        } else {
            // Election timeout occurred, transition to candidate state
            raftState_ = RaftState::Candidate;
            state_.currentTerm++;
            state_.votedFor = serverId_;

            // Reset the election timeout for the next election
            electionTimeout_ = getRandomElectionTimeout();
            timerGeneration_++;

            // Note: In a complete implementation, we would also send RequestVote RPCs to other servers here.
        }
    }
}

std::chrono::milliseconds RaftServer::getRandomElectionTimeout() const {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(150, 300); // Random timeout between 150ms and 300ms
    return std::chrono::milliseconds(distribution(generator));
}