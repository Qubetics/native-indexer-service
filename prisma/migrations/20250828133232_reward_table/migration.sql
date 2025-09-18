-- CreateTable
CREATE TABLE "Rewards" (
    "id" SERIAL NOT NULL,
    "address" TEXT NOT NULL,
    "timeStamp" TIMESTAMP(3) NOT NULL,
    "rewardAmount" TEXT NOT NULL,

    CONSTRAINT "Rewards_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "Rewards_address_timeStamp_idx" ON "Rewards"("address", "timeStamp");

-- CreateIndex
CREATE UNIQUE INDEX "Rewards_address_timeStamp_key" ON "Rewards"("address", "timeStamp");
