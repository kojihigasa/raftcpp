#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <queue>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

struct LogEntry {
    std::string command;
    std::uint64_t term;
};

struct State {
    std::uint64_t currentTerm = 0;
    std::optional<std::string> votedFor;
    std::vector<LogEntry> log;
    // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
    std::uint32_t commitIndex = 0;
    std::uint32_t lastApplied = 0;
    std::vector<std::uint32_t> nextIndex;
    std::vector<std::uint32_t> matchIndex;

    void initializeLeaderState(std::size_t serverCount) {
        const std::uint32_t nextLogIndex = static_cast<std::uint32_t>(log.size()) + 1;
        nextIndex.assign(serverCount, nextLogIndex);
        matchIndex.assign(serverCount, 0);
    }
};

enum class RaftState : int {
    Follower,
    Candidate,
    Leader
};

class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    // This method should be implemented by the user to apply a log entry to the state machine.
    virtual bool applyLogEntry(const LogEntry& entry) = 0;
};

struct RequestVoteArgs {
    std::uint64_t term;
    std::string candidateId;
    std::uint32_t lastLogIndex;
    std::uint64_t lastLogTerm;
};

struct RequestVoteResults {
    std::uint64_t term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    std::uint64_t term;
    std::string leaderId;
    std::uint32_t prevLogIndex;
    std::uint64_t prevLogTerm;
    std::vector<LogEntry> entries;
    std::uint32_t leaderCommit;
};

struct AppendEntriesResults {
    std::uint64_t term;
    bool success;
};

class RaftServer {
public:
    RaftServer(State& state, StateMachine& stateMachine);
    ~RaftServer();

    const std::string& getServerId() const noexcept {
        return serverId_;
    }

    RequestVoteResults handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesResults handleAppendEntries(const AppendEntriesArgs& args);

private:
    void applyLoop();
    void enqueueCommittedEntries();

    void electionTimeoutLoop();
    std::chrono::milliseconds getRandomElectionTimeout() const;

    static std::string generateUniqueServerId();

    State& state_;
    StateMachine& stateMachine_;

    RaftState raftState_ = RaftState::Follower;

    // Unique identifier for the server
    std::string serverId_;
    // Flag to indicate that the RaftServer is being destroyed and the applyLoop should terminate
    bool terminating_ = false;

    // Private members for asynchronous application of committed log entries to the state machine
    std::mutex applyMutex_;
    std::condition_variable applyCv_;
    std::queue<std::pair<std::uint32_t, LogEntry>> applyQueue_;
    std::uint32_t lastEnqueued_ = 0; // The index of the last log entry that has been enqueued for application to the state machine
    std::optional<std::uint32_t> applyFailed_; // The index of the first log entry that failed to be applied to the state machine
    std::thread applyThread_; // Declare at the end to ensure proper construction order

    // Private memebers for election timeout detection
    std::mutex electionMutex_;
    std::condition_variable electionCv_;
    std::chrono::milliseconds electionTimeout_ = std::chrono::milliseconds(150); // Base election timeout duration
    std::uint64_t timerGeneration_ = 0; // A counter to track the number of times the election timer has been reset
    std::thread electionTimeoutThread_; // Declare at the end to ensure proper construction order
};