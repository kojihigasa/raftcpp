#include "../src/raft.hpp"
#include <gtest/gtest.h>

TEST(RequestVote, VoteGrantedWhenCandidateLogHasHigherTermThanReceiverLog) {
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

TEST(RequestVote, VoteGrantedWhenCandidateLogHasSameTermButHigherIndexThanReceiverLog) {
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

TEST(RequestVote, VoteDeniedWhenCandidateLogHasLowerTermThanReceiverLog) {
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