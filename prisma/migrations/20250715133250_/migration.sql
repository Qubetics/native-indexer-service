/*
  Warnings:

  - Made the column `type` on table `Sessions` required. This step will fail if there are existing NULL values in that column.

*/
-- AlterTable
ALTER TABLE "Sessions" ALTER COLUMN "type" SET NOT NULL;
