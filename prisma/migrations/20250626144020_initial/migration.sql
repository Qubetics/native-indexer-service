-- CreateTable
CREATE TABLE "Users" (
    "id" SERIAL NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    "balance" TEXT NOT NULL DEFAULT '0',
    "txn" BIGINT NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Blocks" (
    "id" SERIAL NOT NULL,
    "blocknumber" BIGINT NOT NULL,
    "blockhash" VARCHAR(255) NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "transactionCount" BIGINT NOT NULL DEFAULT 0,
    "validatorOperatorAddress" TEXT NOT NULL,
    "miner" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Blocks_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Transactions" (
    "id" SERIAL NOT NULL,
    "type" TEXT NOT NULL DEFAULT '',
    "status" TEXT NOT NULL DEFAULT '',
    "txString" TEXT NOT NULL DEFAULT '',
    "blocknumber" BIGINT NOT NULL,
    "txhash" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "contractAddress" TEXT NOT NULL DEFAULT 'N/A',
    "fromAddress" VARCHAR(255) NOT NULL,
    "toAddress" VARCHAR(255) NOT NULL,
    "gasWanted" BIGINT NOT NULL DEFAULT 0,
    "gasUsed" BIGINT NOT NULL DEFAULT 0,
    "txFee" TEXT NOT NULL DEFAULT '',
    "value" TEXT NOT NULL DEFAULT '0',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Transactions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Contracts" (
    "id" SERIAL NOT NULL,
    "creator" TEXT NOT NULL DEFAULT '',
    "address" TEXT NOT NULL,
    "blockNumber" BIGINT NOT NULL,
    "txHash" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "contractName" TEXT NOT NULL,
    "contractType" TEXT NOT NULL DEFAULT 'UNKNOWN',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Contracts_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Tokens" (
    "id" SERIAL NOT NULL,
    "creator" TEXT NOT NULL,
    "decimal" TEXT NOT NULL,
    "totalSupply" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "tokenName" TEXT NOT NULL,
    "tokenSymbol" TEXT NOT NULL,
    "contractAddress" TEXT NOT NULL,
    "blockNumber" BIGINT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Tokens_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CoinmarketInfo" (
    "id" SERIAL NOT NULL,
    "price" TEXT NOT NULL,
    "volume24h" TEXT NOT NULL,
    "volumeChange24h" TEXT NOT NULL,
    "percentChange1h" TEXT NOT NULL,
    "percentChange24h" TEXT NOT NULL,
    "percentChange7d" TEXT NOT NULL,
    "percentChange30d" TEXT NOT NULL,
    "symbol" VARCHAR(255) NOT NULL DEFAULT '',
    "currency" VARCHAR(255) NOT NULL DEFAULT '',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CoinmarketInfo_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Validator" (
    "id" SERIAL NOT NULL,
    "operatorAddress" TEXT NOT NULL,
    "validatorAddress" TEXT NOT NULL,
    "jailed" BOOLEAN NOT NULL DEFAULT false,
    "tokens" TEXT NOT NULL DEFAULT '',
    "unbondingHeight" BIGINT NOT NULL,
    "unbondingAmount" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "unbondingTime" TEXT NOT NULL DEFAULT '',
    "totalRewards" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "name" TEXT NOT NULL DEFAULT '',
    "identity" TEXT NOT NULL DEFAULT '',
    "website" TEXT NOT NULL DEFAULT '',
    "SecurityContact" TEXT NOT NULL DEFAULT '',
    "details" TEXT NOT NULL DEFAULT '',
    "status" TEXT NOT NULL DEFAULT '',
    "totalStake" TEXT NOT NULL DEFAULT '',
    "selfStake" TEXT NOT NULL DEFAULT '',
    "commissionRate" TEXT NOT NULL DEFAULT '',
    "commissionMaxRate" TEXT NOT NULL DEFAULT '',
    "commissionUpdateTime" TEXT NOT NULL DEFAULT '',
    "commissionMaxChangeRate" TEXT NOT NULL DEFAULT '',
    "minSelfDelegation" TEXT NOT NULL DEFAULT '',
    "unbondingOnHoldRefCount" TEXT NOT NULL DEFAULT '',
    "unbondingIds" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "delegatorCount" INTEGER NOT NULL,
    "votingPower" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Validator_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ContractLogs" (
    "id" SERIAL NOT NULL,
    "txHash" TEXT NOT NULL,
    "contractAddress" TEXT NOT NULL,
    "blockNumber" BIGINT NOT NULL,
    "txFee" TEXT NOT NULL,
    "logs" TEXT NOT NULL DEFAULT '',
    "method" TEXT NOT NULL DEFAULT 'N/A',
    "status" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ContractLogs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Delegator" (
    "id" SERIAL NOT NULL,
    "totalStake" DECIMAL(65,30) NOT NULL,
    "address" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Delegator_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ValidatorStake" (
    "id" SERIAL NOT NULL,
    "stake" DECIMAL(65,30) NOT NULL,
    "validatorOperatorAddress" TEXT NOT NULL DEFAULT '',
    "delegatorAddress" TEXT NOT NULL,
    "balanceAmount" DECIMAL(65,30) NOT NULL,
    "denom" TEXT NOT NULL,

    CONSTRAINT "ValidatorStake_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Holders" (
    "id" SERIAL NOT NULL,
    "address" TEXT NOT NULL,
    "contractAddress" TEXT NOT NULL,
    "tokenBalance" TEXT NOT NULL,

    CONSTRAINT "Holders_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ValidatedBlocksCount" (
    "operatorAddress" TEXT NOT NULL,
    "address" TEXT NOT NULL,
    "count" BIGINT NOT NULL DEFAULT 0
);

-- CreateTable
CREATE TABLE "Proposals" (
    "id" SERIAL NOT NULL,
    "proposalId" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "totalDeposit" TEXT NOT NULL,
    "metaData" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "summary" TEXT NOT NULL,
    "proposer" TEXT NOT NULL,
    "proposerType" TEXT NOT NULL,
    "tally" TEXT,
    "votingStartTime" TEXT NOT NULL,
    "votingEndTime" TEXT NOT NULL,

    CONSTRAINT "Proposals_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Voters" (
    "id" SERIAL NOT NULL,
    "proposalId" TEXT NOT NULL,
    "txHash" TEXT NOT NULL,
    "answer" TEXT NOT NULL,
    "blockNumber" BIGINT NOT NULL,
    "voter" TEXT NOT NULL,

    CONSTRAINT "Voters_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Users_address_key" ON "Users"("address");

-- CreateIndex
CREATE UNIQUE INDEX "Blocks_blocknumber_key" ON "Blocks"("blocknumber");

-- CreateIndex
CREATE UNIQUE INDEX "Blocks_blockhash_key" ON "Blocks"("blockhash");

-- CreateIndex
CREATE INDEX "Blocks_miner_blockhash_blocknumber_idx" ON "Blocks"("miner", "blockhash", "blocknumber");

-- CreateIndex
CREATE UNIQUE INDEX "Blocks_blocknumber_blockhash_key" ON "Blocks"("blocknumber", "blockhash");

-- CreateIndex
CREATE UNIQUE INDEX "Transactions_txhash_key" ON "Transactions"("txhash");

-- CreateIndex
CREATE INDEX "Transactions_blocknumber_txhash_idx" ON "Transactions"("blocknumber", "txhash");

-- CreateIndex
CREATE UNIQUE INDEX "Contracts_address_key" ON "Contracts"("address");

-- CreateIndex
CREATE UNIQUE INDEX "Tokens_contractAddress_key" ON "Tokens"("contractAddress");

-- CreateIndex
CREATE UNIQUE INDEX "Validator_operatorAddress_key" ON "Validator"("operatorAddress");

-- CreateIndex
CREATE UNIQUE INDEX "Validator_validatorAddress_key" ON "Validator"("validatorAddress");

-- CreateIndex
CREATE INDEX "Validator_selfStake_createdAt_updatedAt_validatorAddress_idx" ON "Validator"("selfStake", "createdAt", "updatedAt", "validatorAddress");

-- CreateIndex
CREATE UNIQUE INDEX "ContractLogs_txHash_key" ON "ContractLogs"("txHash");

-- CreateIndex
CREATE UNIQUE INDEX "Delegator_address_key" ON "Delegator"("address");

-- CreateIndex
CREATE INDEX "TotalStakeIndex" ON "Delegator"("totalStake");

-- CreateIndex
CREATE INDEX "validatorOperatorAddress" ON "ValidatorStake"("validatorOperatorAddress");

-- CreateIndex
CREATE UNIQUE INDEX "ValidatedBlocksCount_operatorAddress_key" ON "ValidatedBlocksCount"("operatorAddress");

-- CreateIndex
CREATE UNIQUE INDEX "ValidatedBlocksCount_address_key" ON "ValidatedBlocksCount"("address");

-- CreateIndex
CREATE INDEX "ValidatedBlocksCount_operatorAddress_idx" ON "ValidatedBlocksCount"("operatorAddress");

-- CreateIndex
CREATE UNIQUE INDEX "Proposals_proposalId_key" ON "Proposals"("proposalId");

-- CreateIndex
CREATE INDEX "Proposals_proposalId_proposerType_status_idx" ON "Proposals"("proposalId", "proposerType", "status");

-- CreateIndex
CREATE UNIQUE INDEX "Voters_txHash_key" ON "Voters"("txHash");

-- CreateIndex
CREATE INDEX "Voters_proposalId_answer_voter_idx" ON "Voters"("proposalId", "answer", "voter");

-- CreateIndex
CREATE UNIQUE INDEX "Voters_proposalId_voter_key" ON "Voters"("proposalId", "voter");

-- AddForeignKey
ALTER TABLE "Tokens" ADD CONSTRAINT "Tokens_contractAddress_fkey" FOREIGN KEY ("contractAddress") REFERENCES "Contracts"("address") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ContractLogs" ADD CONSTRAINT "ContractLogs_contractAddress_fkey" FOREIGN KEY ("contractAddress") REFERENCES "Contracts"("address") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ContractLogs" ADD CONSTRAINT "ContractLogs_txHash_fkey" FOREIGN KEY ("txHash") REFERENCES "Transactions"("txhash") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ValidatorStake" ADD CONSTRAINT "ValidatorStake_delegatorAddress_fkey" FOREIGN KEY ("delegatorAddress") REFERENCES "Delegator"("address") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Holders" ADD CONSTRAINT "Holders_contractAddress_fkey" FOREIGN KEY ("contractAddress") REFERENCES "Contracts"("address") ON DELETE RESTRICT ON UPDATE CASCADE;
