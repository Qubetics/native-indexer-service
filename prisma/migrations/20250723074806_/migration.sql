/*
  Warnings:

  - Added the required column `blocknumber` to the `Sessions` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "Sessions" ADD COLUMN     "blocknumber" BIGINT NOT NULL,
ALTER COLUMN "price" SET DATA TYPE TEXT;
