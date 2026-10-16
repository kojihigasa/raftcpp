#pragma once

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
    // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
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

RequestVoteResults handleRequestVote(const RequestVoteArgs& args, State& state);
AppendEntriesResults handleAppendEntries(const AppendEntriesArgs& args, State& state);