import * as amqp from "amqplib/callback_api";
import { log } from "../Utility/common";
import { COLORS, QUEUE_NAME, REDIS_KEY } from "../constant";
import dbHelper from "../helper/db.helper";
import processHelper from "../helper/process.helper";
import { IValidator } from "../interface";
import { prisma } from "../lib/db";
import logger from "../lib/logger";
import { RedisService } from ".";

class RabbitMq {
  public channel: amqp.Channel;

  constructor() {
    this.startServer();
  }

  public async startServer() {
    try {
      const res = await this.connect();
      this.channel = res;
      Object.values(QUEUE_NAME).forEach((queue) => {
        this.channel.assertQueue(queue, {
          durable: false,
        });
      });

      this.channel.prefetch(7);
      this.consumeMissedTxs();
      this.consumeErrorBlocks();
      this.consumeLatestBlocks();
      this.consumeMissedBlocks();
      this.consumeDownTimeBlocks();
      this.consumeDownTransaction();
      // this.consumeValidatorDetails();
      this.consumeLatestTransaction();

      return true;
    } catch (error) {
      logger.error("Error while connecting to RabbitMQ: ", error);
      return false;
    }
  }

  public connect(): Promise<amqp.Channel> {
    const RECONNECT_DELAY = 5000;
    return new Promise((resolve, reject) => {
      const attemptConnection = () => {
        amqp.connect(environment.rabbitMq, (err, connection) => {
          if (err) {
            logger.error("Error connecting to RabbitMQ:", err);
            reject(err);
            return;
          }

          connection.on("error", (error) => {
            logger.error("RabbitMQ connection error:", error.message);
            setTimeout(attemptConnection, RECONNECT_DELAY);
          });

          connection.createChannel((error: Error, channel) => {
            if (error) {
              logger.error("Error creating channel:", error);
              reject(error);
              return;
            }
            log("RabbitMQ Conneted ", COLORS.yellow, "true", COLORS.green);
            resolve(channel);
          });
        });
      };

      attemptConnection();
    });
  }

  /**
   * Save Data in queue
   * @param queue
   * @param data
   * @returns
   */
  public inQueueData(queue: string, data: string) {
    try {
      this.channel?.sendToQueue(queue, Buffer.from(data));
      return true;
    } catch (error) {
      console.log("error inside inQueueData : ", error);
      return false;
    }
  }

  /**
   * For consuming all type of transactions
   */
  public consumeLatestTransaction() {
    try {
      this.channel?.consume(
        QUEUE_NAME.LATEST_TXS,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const txData = JSON.parse(message.content.toString());
              const txRes = await processHelper.processTxs(txData);
              // await new Promise((res) => setTimeout(res, 6 * 60 * 1000));
              if (!txRes) {
                if (txData?.result?.events) {
                  const tx = JSON.stringify(txData);
                  // console.log(
                  //   'txHash data inside not saved case in queue : ',
                  //   tx,
                  // );
                  this.inQueueData(QUEUE_NAME.MISSED_TXS, tx);
                }
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** consumeLatestTransaction Error", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log(
        'error while consuming all consumeLatestTransaction : ',
        error
      );
    }
  }

  /**
   * For consuming all type of transactions
   */
  public consumeDownTransaction() {
    try {
      this.channel?.consume(
        QUEUE_NAME.DOWNTIME_TXS,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const txHash = message.content.toString();
              const txRes = await processHelper.processMissedTxs(txHash);
              if (!txRes) {
                this.inQueueData(QUEUE_NAME.DOWNTIME_TXS, txHash);
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** consumeLatestTransaction Error", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false }
        
      );
    } catch (error) {
      console.log("error while consuming all consumeDownTransaction : ", error);
    }
  }

  /**
   * For consuming all type of transactions
   */
  public consumeLatestBlocks() {
    try {
      this.channel?.consume(
        QUEUE_NAME.LATEST_BLOCKS,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const blockData = JSON.parse(message.content.toString());

              const issaved = await processHelper.processLatestBlock(blockData);

              if (!issaved) {
                if (blockData?.result?.data) {
                  const blockNumber =
                    blockData?.result?.data?.value?.block?.header?.height;
                  if (blockNumber) {
                    this.inQueueData(
                      QUEUE_NAME.ERROR_BLOCK,
                      blockNumber.toString(),
                    );
                  }
                }
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** #error consumeLatestBlocks", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("### Error consumeLatestBlocks ", error);
    }
  }

  /**
   * For consuming all type of transactions
   */
  public consumeMissedTxs() {
    try {
      this.channel?.consume(
        QUEUE_NAME.MISSED_TXS,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const txData = JSON.parse(message.content.toString());
              const txRes = await processHelper.processTxs(txData);
              // await new Promise((res) => setTimeout(res, 6 * 60 * 1000));
              if (!txRes) {
                this.inQueueData(QUEUE_NAME.MISSED_TXS, JSON.stringify(txData));
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** Error consumeMissedTxs", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("### Error consumeMissedTxs", error);
    }
  }

  /**
   * For consuming the error blocks
   */
  public consumeErrorBlocks(): void {
    try {
      this.channel?.consume(
        QUEUE_NAME.ERROR_BLOCK,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const blockNo = message.content.toString();
              await processHelper.processErrorBlock(blockNo);
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** error consumeErrorBlocks", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("### error consumeErrorBlocks", error);
    }
  }

  /**
   * For consuming the Missed blocks
   */
  public consumeMissedBlocks(): void {
    try {
      this.channel?.consume(
        QUEUE_NAME.MISSED_BLOCK,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const blockData = JSON.parse(message.content.toString());

              const isSaved =
                await processHelper.processMissedBlocks(blockData);

              if (!isSaved) {
                if (blockData?.blocknumber) {
                  this.inQueueData(
                    QUEUE_NAME.ERROR_BLOCK,
                    blockData.blocknumber.toString(),
                  );
                }
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** #consumeMissedBlocks: ", error);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("Error consumeMissedBlocks", error);
    }
  }

  /**
   * Handle blocks one by one when server is down
   */
  consumeDownTimeBlocks(): void {
    try {
      this.channel?.consume(
        QUEUE_NAME.DOWNTIME_BLOCK,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const block = message.content.toString();
              const isSuccess = await processHelper.processErrorBlock(block);
              if (isSuccess) {
                // Retrieve the block count from Redis or the database
                let blockCount = await RedisService.getString(
                  REDIS_KEY.NATIVE_BLOCKS_COUNT,
                );

                if (!blockCount) {
                  // First time, get the count from the database
                  blockCount = await prisma.blocks.count();
                  await RedisService.setString(
                    REDIS_KEY.NATIVE_BLOCKS_COUNT,
                    blockCount,
                  );
                } else {
                  // Increment the block count
                  blockCount = Number(blockCount) + 1;
                  await RedisService.setString(
                    REDIS_KEY.NATIVE_BLOCKS_COUNT,
                    blockCount,
                  );
                }

                await RedisService.setString(
                  REDIS_KEY.LATEST_BLOCK,
                  Number(block),
                );
              }

              // log(
              //   "downtime block : ",
              //   COLORS.cyan,
              //   `${block} ${isSuccess}`,
              //   COLORS.yellow,
              // );
              if (!isSuccess) {
                this.inQueueData(QUEUE_NAME.ERROR_BLOCK, block.toString());
              }
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** Error consumeDownTimeBlocks", error);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("### Error consumeDownTimeBlocks", error);
    }
  }

  public consumeNullTimestamp() {
    try {
      this.channel?.consume(
        QUEUE_NAME.NULL_TIMESTAMP,
        async (message: amqp.Message | null) => {
          if (message) {
            try {
              const blockData = JSON.parse(message.content.toString());
              await processHelper.processLatestBlock(blockData);
              this.channel?.ack(message);
            } catch (error) {
              console.log("*** consumeNullTimestamp", error);
              this.channel?.ack(message);
            }
          }
        },
        { noAck: false },
      );
    } catch (error) {
      console.log("### error consumeNullTimestamp", error);
    }
  }
}

export default new RabbitMq();
