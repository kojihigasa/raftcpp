#pragma once

#include <condition_variable>
#include <cstddef>
#include <queue>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
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

class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    // This method should be implemented by the user to apply a log entry to the state machine.
    virtual bool applyLogEntry(const LogEntry& entry) = 0;
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

class RaftNode {
public:
    RaftNode(State& state, StateMachine& stateMachine);
    ~RaftNode();

    RequestVoteResults handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesResults handleAppendEntries(const AppendEntriesArgs& args);

private:
    void applyLoop();
    void enqueueCommittedEntries();

    State& state_;
    StateMachine& stateMachine_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::pair<int, LogEntry>> applyQueue_;

    int lastEnqueued_ = 0;
    bool terminating_ = false;
    std::optional<int> applyFailed_;

    // Declare at the end to ensure proper construction order
    std::thread applyThread_;
};