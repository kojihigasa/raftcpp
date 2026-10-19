#include "../src/raft.hpp"
#include <gtest/gtest.h>

TEST(RequestVote, VoteDeniedWhenCandidateLogHasLowerTermThanFollowerLog) {
    State state;
    state.currentTerm = 2;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 2}};

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = 1;
    args.lastLogIndex = 1;
    args.lastLogTerm = 1;

    RequestVoteResults results = handleRequestVote(args, state);

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

    RequestVoteArgs args;
    args.term = 2;
    args.candidateId = 1;
    args.lastLogIndex = 1;
    args.lastLogTerm = 1;

    RequestVoteResults results = handleRequestVote(args, state);

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

    RequestVoteArgs args;
    args.term = 1;
    args.candidateId = 1;
    args.lastLogIndex = 2;
    args.lastLogTerm = 1;

    RequestVoteResults results = handleRequestVote(args, state);

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

    AppendEntriesArgs args;
    args.term = 1; // Lower term than currentTerm
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 2; // This index does not exist in the receiver's log
    args.prevLogTerm = 1;
    args.entries = {{"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 2; // This index exists in the receiver's log
    args.prevLogTerm = 2; // But the term does not match
    args.entries = {{"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;
    args.entries = {{"command2", 1}, {"command3", 1}};
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {{"command2b", 2}}; // Conflicting entry (different term)
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

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

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {{"command2b", 2}, {"command3b", 2}}; // Conflicting entries (different term)
    args.leaderCommit = 0;

    AppendEntriesResults results = handleAppendEntries(args, state);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 3); // Log should be truncated to the conflicting entries
    EXPECT_EQ(state.log[1].command, "command2b");
    EXPECT_EQ(state.log[2].command, "command3b");
}

TEST(AppendEntries, CommitIndexUpdatedToLeaderCommitIndexWhenLeaderCommitIndexIsGreaterThanFollowerCommitIndex) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}, {"command2", 1}}; // Follower has same entries as leader has committed
    state.commitIndex = 0;

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 2; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {}; // No new entries
    args.leaderCommit = 2; // Leader's commit index is greater than follower's commit index

    AppendEntriesResults results = handleAppendEntries(args, state);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 2); // Follower's commit index should be updated to leader's commit index
}

TEST(AppendEntries, CommitIndexUpdatedToLastLogIndexWhenLeaderCommitIndexIsGreaterThanFollowerCommitIndexAndFollowerHasFewerEntriesThanLeader) {
    State state;
    state.currentTerm = 1;
    state.votedFor = std::nullopt;
    state.log = {{"command1", 1}}; // Follower has fewer entries than leader has committed
    state.commitIndex = 0;

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1; // This index exists in the receiver's log
    args.prevLogTerm = 1; // And the term matches
    args.entries = {{"command2", 1}, {"command3", 1}}; // Leader has more new entries already committed
    args.leaderCommit = 3; // Leader's commit index is greater than follower's commit index

    AppendEntriesResults results = handleAppendEntries(args, state);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 3); // Follower's commit index should be updated to the last log index (3)
}