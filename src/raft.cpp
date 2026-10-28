#include "raft.hpp"
#include <algorithm>

RaftNode::RaftNode(State& state, StateMachine& stateMachine)
    : state_(state),
      stateMachine_(stateMachine),
      lastEnqueued_(state.lastApplied) {
    // Initialize at the body of the constructor to ensure that the applyThread_ is started after all member variables are initialized.
    applyThread_ = std::thread(&RaftNode::applyLoop, this);
}

RaftNode::~RaftNode() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        terminating_ = true;
    }

    cv_.notify_one();

    if (applyThread_.joinable()) {
        applyThread_.join();
    }
}

RequestVoteResults RaftNode::handleRequestVote(const RequestVoteArgs& args) {
    if (args.term > state_.currentTerm) {
        state_.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state_.votedFor.reset();
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
                        args.lastLogIndex >= static_cast<int>(state_.log.size()));

    if ((!state_.votedFor.has_value() || state_.votedFor.value() == args.candidateId) && logUpToDate) {
        state_.votedFor = args.candidateId;
        results.voteGranted = true;
    } else {
        results.voteGranted = false;
    }

    return results;
}

AppendEntriesResults RaftNode::handleAppendEntries(const AppendEntriesArgs& args) {
    if (args.term > state_.currentTerm) {
        state_.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state_.votedFor.reset();
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
    if (args.prevLogIndex > 0 && (args.prevLogIndex > static_cast<int>(state_.log.size()) ||
                                  state_.log[args.prevLogIndex - 1].term != args.prevLogTerm)) {
        results.success = false;
        return results;
    }

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    for (std::size_t i = 0; i < args.entries.size(); i++) {
        int logIndex = args.prevLogIndex + 1 + static_cast<int>(i);
        if (logIndex <= static_cast<int>(state_.log.size())) {
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
        state_.commitIndex = std::min(args.leaderCommit, static_cast<int>(state_.log.size()));
    }

    // Enqueue the newly committed entries to be applied to the state machine asynchronously
    // and get back to the leader immediately without waiting for the apply to complete.
    enqueueCommittedEntries();

    results.success = true;
    return results;
}

void RaftNode::applyLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] {
            return terminating_ || (!applyQueue_.empty() && !applyFailed_.has_value());
        });

        if (terminating_ && applyQueue_.empty()) {
            break;
        }

        auto [index, entry] = applyQueue_.front();
        applyQueue_.pop_front();
        lock.unlock();

        if (!stateMachine_.applyLogEntry(entry)) {
            std::lock_guard<std::mutex> lock(mutex_);
            applyFailed_ = index;
            break;
        }

        state_.lastApplied = index;
    }
}

void RaftNode::enqueueCommittedEntries() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        while (lastEnqueued_ < state_.commitIndex) {
            const int index = lastEnqueued_ + 1;
            applyQueue_.emplace_back(index, state_.log[index - 1]);
            lastEnqueued_ = index;
        }
    }

    cv_.notify_one();
}