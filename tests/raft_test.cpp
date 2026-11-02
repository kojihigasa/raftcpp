#include "../src/raft.hpp"
#include <chrono>
#include <future>
#include <gtest/gtest.h>

// Empty state machine implementation for RPC testing purposes.
class EmptyStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry& entry) override {
        // For testing purposes, we can just return true to indicate that the log entry was applied
        return true;
    }
};

TEST(RequestVote, VoteDeniedWhenCandidateLogHasLowerTermThanFollowerLog) {
    State state;
    state.currentTerm = 2;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 2}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    RequestVoteArgs args{};
    args.term = 1;
    args.candidateId = 1;
    args.lastLogIndex = 1;
    args.lastLogTerm = 1;

    RequestVoteResults results = raftServer.handleRequestVote(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_FALSE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.votedFor, std::nullopt);
}

TEST(RequestVote, VoteGrantedWhenCandidateLogHasHigherTermThanFollowerLog) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    RequestVoteArgs args{};
    args.term = 2;
    args.candidateId = 1;
    args.lastLogIndex = 1;
    args.lastLogTerm = 1;

    RequestVoteResults results = raftServer.handleRequestVote(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_TRUE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.votedFor.value(), 1);
}

TEST(RequestVote, VoteGrantedWhenCandidateLogHasSameTermButHigherIndexThanFollowerLog) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    RequestVoteArgs args{};
    args.term = 1;
    args.candidateId = 1;
    args.lastLogIndex = 2;
    args.lastLogTerm = 1;

    RequestVoteResults results = raftServer.handleRequestVote(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.votedFor.value(), 1);
}

TEST(AppendEntries, EntriesNotAppendedWhenLeaderTermIsLowerThanFollowerTerm) {
    State state;
    state.currentTerm = 2;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 2}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1; // Lower term than currentTerm
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.log.size(), 1); // Log should remain unchanged
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatch) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2);
    EXPECT_EQ(state.log[1].command, "command2");
}

TEST(AppendEntries, EntriesNotAppendedWhenPrevLogIndexDoesNotExist) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 2; // This index does not exist in the receiver's log
    args.prevLogTerm = 1;
    args.entries = {{"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 1); // Log should remain unchanged
}

TEST(AppendEntries, EntriesNotAppendedWhenPrevLogTermDoesNotMatch) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}, {"command2", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 2; // This index exists in the receiver's log
    args.prevLogTerm = 2; // But the term does not match
    args.entries = {{"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2); // Log should remain unchanged
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithMultipleEntries) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}, {"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 3);
    EXPECT_EQ(state.log[1].command, "command2");
    EXPECT_EQ(state.log[2].command, "command3");
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithConflictingEntry) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}, {"command2a", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {{"command2b", 2}}; // Conflicting entry (different term)
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2); // Log should be truncated to the conflicting entry
    EXPECT_EQ(state.log[1].command, "command2b");
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithMultipleConflictingEntries) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}, {"command2a", 1}, {"command3a", 1}};
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {{"command2b", 2}, {"command3b", 2}}; // Conflicting entries (different term)
    args.leaderCommit = 0;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 3); // Log should be truncated to the conflicting entries
    EXPECT_EQ(state.log[1].command, "command2b");
    EXPECT_EQ(state.log[2].command, "command3b");
}

TEST(AppendEntries, CommitIndexUpdatedToLeaderCommitIndexWhenLeaderCommitIndexIsSmallerThanIndexOfFollowerLastNewEntry) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}, {"command2", 1}};
    state.commitIndex = 0;
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command3", 1}};
    // Leader's commit index (2) is greater than follower's commit index (0) and is smaller than index of follower's new entry (3).
    args.leaderCommit = 2;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 2); // Follower's commit index should be updated to leader's commit index (2)
}

TEST(AppendEntries, CommitIndexUpdatedToIndexOfFollowerLastNewEntryWhenIndexOfFollowerLastNewEntryIsSmallerThanLeaderCommitIndex) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}};
    state.commitIndex = 0;
    EmptyStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}};
    // Leader's commit index (3) is greater than follwer's commit index (0) and index of follower's new entry (2).
    args.leaderCommit = 3;

    AppendEntriesResults results = raftServer.handleAppendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 2); // Follower's commit index should be updated to index of follower's new entry (2)
}

class RecordingStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry& entry) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // Pseudo apply the log entry to the state machine by recording the command
            appliedCommands_.push_back(entry.command);
        }
        cv_.notify_all();
        return true;
    }

    bool waitForAppliedCount(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, count] {
            return appliedCommands_.size() >= count;
        });
    }

    std::vector<std::string> appliedCommands() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return appliedCommands_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> appliedCommands_;
};

TEST(AsyncApply, AppliesCommittedEntriesInOrderWithoutDuplicates) {
    State state;
    state.currentTerm = 1;
    RecordingStateMachine stateMachine;

    {
        RaftServer raftServer(state, stateMachine);

        AppendEntriesArgs firstAppend{};
        firstAppend.term = 1;
        firstAppend.leaderId = 1;
        firstAppend.entries = {{"command1", 1}, {"command2", 1}};
        firstAppend.leaderCommit = 1;

        AppendEntriesResults firstResults = raftServer.handleAppendEntries(firstAppend);
        ASSERT_TRUE(firstResults.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(1, std::chrono::seconds(1)));

        AppendEntriesArgs commitSecondEntry{};
        commitSecondEntry.term = 1;
        commitSecondEntry.leaderId = 1;
        commitSecondEntry.prevLogIndex = 2;
        commitSecondEntry.prevLogTerm = 1;
        commitSecondEntry.leaderCommit = 2;

        AppendEntriesResults secondResults = raftServer.handleAppendEntries(commitSecondEntry);
        EXPECT_TRUE(secondResults.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(2, std::chrono::seconds(1)));

        AppendEntriesResults repeatedResults = raftServer.handleAppendEntries(commitSecondEntry);
        EXPECT_TRUE(repeatedResults.success);
    }

    EXPECT_EQ(state.lastApplied, 2);
    EXPECT_EQ(stateMachine.appliedCommands(), (std::vector<std::string>{"command1", "command2"}));
}

class BlockingStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry&) override {
        std::unique_lock<std::mutex> lock(mutex_);
        started_ = true;
        cv_.notify_all();
        cv_.wait(lock, [this] {
            return released_;
        });
        return true;
    }

    bool waitUntilStarted(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] {
            return started_;
        });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool started_ = false;
    bool released_ = false;
};

TEST(AsyncApply, AppendEntriesReturnsWhileStateMachineIsBlocked) {
    State state;
    state.currentTerm = 1;
    BlockingStateMachine stateMachine;
    RaftServer raftServer(state, stateMachine);

    AppendEntriesArgs args{};
    args.term = 1;
    args.leaderId = 1;
    args.entries = {{"command1", 1}};
    args.leaderCommit = 1;

    auto result = std::async(std::launch::async, [&raftServer, args] {
        return raftServer.handleAppendEntries(args);
    });

    const bool applyStarted = stateMachine.waitUntilStarted(std::chrono::seconds(1));
    const bool returnedWhileBlocked = result.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    stateMachine.release();

    const AppendEntriesResults appendResults = result.get();
    EXPECT_TRUE(applyStarted);
    EXPECT_TRUE(returnedWhileBlocked);
    EXPECT_TRUE(appendResults.success);
}