/*
  Warnings:

  - The primary key for the `Sessions` table will be changed. If it partially fails, the table could be left without primary key constraint.
  - The `id` column on the `Sessions` table would be dropped and recreated. This will lead to data loss if there is data in the column.

*/
-- AlterTable
ALTER TABLE "Sessions" DROP CONSTRAINT "Sessions_pkey",
ADD COLUMN     "planId" TEXT,
ADD COLUMN     "sessionId" TEXT,
DROP COLUMN "id",
ADD COLUMN     "id" SERIAL NOT NULL,
ALTER COLUMN "subscriptionId" DROP NOT NULL,
ADD CONSTRAINT "Sessions_pkey" PRIMARY KEY ("id");
