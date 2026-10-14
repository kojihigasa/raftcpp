#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <vector>

struct LogEntry {
    std::string command;
    int term;
};

struct State {
    int currentTerm = 0;
    std::optional<int> votedFor;
    std::vector<LogEntry> log;
    int commitIndex = 0;
    int lastApplied = 0;
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

    void initializeLeaderState(std::size_t serverCount) {
        const int nextLogIndex = static_cast<int>(log.size()) + 1;
        nextIndex.assign(serverCount, nextLogIndex);
        matchIndex.assign(serverCount, 0);
    }
};

struct RequestVoteArgs {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

struct RequestVoteResults {
    int term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};

struct AppendEntriesResults {
    int term;
    bool success;
};

RequestVoteResults handleRequestVote(const RequestVoteArgs& args, State& state) {
    if (args.term > state.currentTerm) {
        state.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state.votedFor.reset();
    }

    RequestVoteResults results;
    results.term = state.currentTerm;

    if (args.term < state.currentTerm) {
        results.voteGranted = false;
        return results;
    }

    // Check if the candidate's log is at least as up-to-date as the receiver's log
    // A log is considered up-to-date if its last log term is greater than the receiver's last log term,
    // or if the last log terms are equal, the candidate's last log index is greater than or equal to the receiver's last log index.
    bool logUpToDate = (args.lastLogTerm > (state.log.empty() ? 0 : state.log.back().term)) ||
                       (args.lastLogTerm == (state.log.empty() ? 0 : state.log.back().term) &&
                        args.lastLogIndex >= static_cast<int>(state.log.size()));

    if ((!state.votedFor.has_value() || state.votedFor.value() == args.candidateId) && logUpToDate) {
        state.votedFor = args.candidateId;
        results.voteGranted = true;
    } else {
        results.voteGranted = false;
    }

    return results;
}

AppendEntriesResults handleAppendEntries(const AppendEntriesArgs& args, State& state) {
    if (args.term > state.currentTerm) {
        state.currentTerm = args.term;
        // Reset votedFor when the term is updated to indicate that the server has not voted for any candidate in the new term.
        state.votedFor.reset();
    }

    AppendEntriesResults results;
    results.term = state.currentTerm;

    if (args.term < state.currentTerm) {
        results.success = false;
        return results;
    }

    // Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
    // Raft maintains the following property to ensure the consistency of logs on different servers:
    // If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.
    if (args.prevLogIndex > 0 && (args.prevLogIndex > static_cast<int>(state.log.size()) ||
                                  state.log[args.prevLogIndex - 1].term != args.prevLogTerm)) {
        results.success = false;
        return results;
    }

    // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    for (std::size_t i = 0; i < args.entries.size(); ++i) {
        int logIndex = args.prevLogIndex + 1 + static_cast<int>(i);
        if (logIndex <= static_cast<int>(state.log.size())) {
            if (state.log[logIndex - 1].term != args.entries[i].term) {
                state.log.resize(logIndex - 1);
                break;
            }
        }
    }

    // Append any new entries not already in the log
    for (const auto& entry : args.entries) {
        state.log.push_back(entry);
    }

    if (args.leaderCommit > state.commitIndex) {
        state.commitIndex = std::min(args.leaderCommit, static_cast<int>(state.log.size()));
    }

    results.success = true;
    return results;
}