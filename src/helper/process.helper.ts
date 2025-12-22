import { BigNumber } from 'bignumber.js';
import { v4 } from 'uuid';
import {
  log,
  fetchRequest,
  noExponent,
  retriveNumbers,
  parseTx,
  cleanEscapedString,
  addHours,
  getType,
  parseTics,
  getActionAndSender,
} from '../Utility/common';
import {
  COLORS,
  HEADERS,
  REDIS_KEY,
  QUEUE_NAME,
  FETCH_METHODS,
  TRANSACTION_TYPE,
  TRANSACTION_STATUS,
  SOCKET_EVENT,
  RES_MSG,
} from '../constant';
import {
  IBlock,
  IContractLogs,
  IContracts,
  ITokens,
  ITransaction,
  IVoters,
} from '../interface';
import { prisma } from '../lib/db';
import { RabbitMqService, RedisService, SocketEventEmitter } from '../service';
import rabbitmqService from '../service/rabbitmq.service';
import redisHelper from '../service/redis.helper';
import socketEventsHelper from '../service/socketEvents.helper';
import { CosmJsRpcMethods } from './cosmsClient.helper';
import ExlorerHelper from './db.helper';
import dbHelper from './db.helper';
import { CustomWebSocket } from './ws.helper';

class ProcessHelper {
  cosmjsInstace: CosmJsRpcMethods;

  constructor() {
    this.cosmjsInstace = CosmJsRpcMethods.getInstance();
  }

  /**
   * Subscribe to latest block
   */
  public subscribeLatestBlocks() {
    const blockWsQuery = {
      jsonrpc: '2.0',
      method: 'subscribe',
      id: v4().toString(),
      params: {
        query: "tm.event='NewBlock'",
      },
    };
    const blockSocket = new CustomWebSocket(
      `${environment.wsUrl}/websocket`,
      QUEUE_NAME.LATEST_BLOCKS,
      blockWsQuery
    );
    blockSocket.connect();
  }

  /**
   * Subscribe to latest Transaction
   */
  public subscribeLatestTxs() {
    const txWsQuery = {
      jsonrpc: '2.0',
      method: 'subscribe',
      id: v4().toString(),
      params: {
        query: "tm.event='Tx'",
      },
    };
    const txSocket = new CustomWebSocket(
      `${environment.wsUrl}/websocket`,
      QUEUE_NAME.LATEST_TXS,
      txWsQuery
    );
    txSocket.connect();
  }

  /**
   * process latest block
   * @param message
   * @returns
   */


  async processDelegatorRewards(
    address: string,
    rewardEvent: string[],
    timeStamp: string
  ) {
    try {
      let rewardAmount = BigNumber(0);

      for (let i = 0; i < rewardEvent?.length; i++) {
        const element = rewardEvent[i];
        let amountToAdd = '0';

        if (
          typeof element === 'string' &&
          element.includes(environment.symbol.toLowerCase())
        ) {
          amountToAdd = element.replace(/[^\d.]/g, '');
        }
        rewardAmount = rewardAmount.plus(amountToAdd || 0);
      }

      console.log('rewardAmount:  ', rewardAmount.toString());

      const rewardData = await prisma.rewards.findFirst({
        where: {
          address,
        },
      });

      if (!rewardData) {
        console.log(
          'Reward not Found in DataBase, so saving....... in db and redis',
          {
            address,
            timeStamp,
            rewardAmount: rewardAmount.toString(),
          }
        );

        await prisma.rewards.create({
          data: {
            address,
            timeStamp,
            rewardAmount: rewardAmount.toString(),
          },
        });

        await redisHelper.setHset(REDIS_KEY.REWARDS, address, {
          timeStamp,
          rewardAmount: rewardAmount.toString(),
        });
      } else {
        const timeCondition =
          new Date(timeStamp).getTime() >
          new Date(rewardData?.timeStamp).getTime();

        console.log(
          'Reward Found in DataBase..... and time not Matched ',
          new Date(timeStamp).getTime(),
          new Date(rewardData?.timeStamp).getTime(),
          timeCondition
        );

        if (timeCondition) {
          const newRewardToSet = rewardAmount
            .plus(rewardData.rewardAmount)
            .toString();

          console.log(
            'Updating new Reward in table and redis ',
            newRewardToSet
          );

          await redisHelper.setHset(REDIS_KEY.REWARDS, address, {
            timeStamp,
            rewardAmount: newRewardToSet,
          });

          await prisma.rewards.update({
            where: { address },
            data: {
              timeStamp,
              rewardAmount: rewardAmount.toString(),
            },
          });
        }
      }
    } catch (error) {
      console.log('Error while calculating rewards : ', error);
    }
  }

  /**
   * process latest transactions
   * @param message
   * @returns
   */


  /**
   * Process and save missed transactions
   * @param txHash of type string
   * @returns
   */
  public async processMissedTxs(txHash: string) {
    try {
      const url = `${environment.httpHost}/tx?hash=0x${txHash}`;
      const fetchRes = await fetchRequest(
        url,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      const swaggerHttp = `${environment.evmHost}/cosmos/tx/v1beta1/txs/${txHash}`;
      const getSwaggerData = await fetchRequest(
        swaggerHttp,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      if (fetchRes?.data?.result && !fetchRes.error) {
        const result = fetchRes?.data?.result;

        const events = result?.tx_result?.events;

        const code = result?.tx_result?.code;
        const status = !code
          ? TRANSACTION_STATUS.SUCCESS
          : TRANSACTION_STATUS.FAILED;

        const txData: ITransaction = {
          status,
          txhash: `0x${txHash}`,
          txString: fetchRes.data?.result?.tx ?? '',
          blocknumber: Number(fetchRes.data?.result?.height ?? 0),
          txFee: fetchRes.data.result?.tx_result?.gas_used ?? '',
          gasWanted: fetchRes.data.result?.tx_result?.gas_wanted ?? '',
          gasUsed: fetchRes.data.result?.tx_result?.gas_used ?? '',
          type: '',
          toAddress: '',
          fromAddress: '',
          value: '',
          timestamp: getSwaggerData?.data?.tx_response?.timestamp.toString(),
          contractAddress: '',
        };

        // const url2 = `${environment.httpHost}/block?height=${txData.blocknumber}`;
        // const fetchRes2 = await fetchRequest(
        //   url2,
        //   FETCH_METHODS.GET,
        //   HEADERS.DEFAULT,
        //   null
        // );

        // console.log("fetchRes2: ", fetchRes2?.error);

        // if (!fetchRes2.error) {
        //   console.log(
        //     "timeStamp fro RPC: ",
        //     fetchRes2?.data.result.block?.header?.time
        //   );

        //   txData.timestamp = fetchRes2?.data.result.block?.header?.time ?? "";
        // }

        let val = 0;

        // Default values
        const transferEvent = events.find((i: any) => i.type === 'transfer');

        transferEvent?.attributes?.forEach((i: any) => {
          switch (i.key) {
            case 'recipient':
              txData.toAddress = i?.value || '';
              break;

            case 'sender':
              console.log('\n From value from transafer ::: ', i?.value);
              txData.fromAddress = i?.value || '';
              break;

            case 'amount': {
              val = i?.value ?? 0;
              break;
            }

            default:
              break;
          }
        });

        const filteredEvents = events.filter(
          (event: any) =>
            event.type === 'message' &&
            event?.attributes.some((attr: any) => attr.key === 'action')
        );

        const eventObj = filteredEvents[0]?.attributes.find(
          (i: any) => i.key === 'action'
        );
        // const fromObj = filteredEvents[0]?.attributes.find(
        //   (i: any) => i.key === 'sender'
        // );

        const txEvent = eventObj?.value || '';
        console.log('Missed txEvent: ', txEvent);

        console.log('\n\ntxEvent :::: ', txEvent);

        switch (txEvent) {
          case '/cosmos.bank.v1beta1.MsgSend':
            txData.type = TRANSACTION_TYPE.COIN_TRANSFER;
            break;

          case '/cosmos.staking.v1beta1.MsgDelegate': {
            txData.type = TRANSACTION_TYPE.BOND_MORE;
            txData.toAddress = '/cosmos.staking.v1beta1.MsgDelegate';

            const delegateEvent = events.find(
              (e: any) => e.type === 'delegate'
            );
            const amountAttr = delegateEvent?.attributes.find(
              (e: any) => e.key === 'amount'
            );
            val = (amountAttr?.value || '0').replace(/[^\d.]/g, '');

            if (events['withdraw_rewards.amount']) {
              this.processDelegatorRewards(
                txData.fromAddress,
                events['withdraw_rewards.amount'],
                txData.timestamp
              );
            }
            break;
          }

          case '/cosmos.staking.v1beta1.MsgBeginRedelegate': {
            txData.type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
            txData.toAddress = '/cosmos.staking.v1beta1.MsgBeginRedelegate';

            const filtered = events.filter((e: any) => e.type === 'redelegate');

            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === 'amount'
            );
            val = (valArr[0].value || '0').replace(/[^\d.]/g, '');

            const filterForReward = events.filter(
              (e: any) => e.type === 'withdraw_rewards'
            );
            const valArrReward = filterForReward[0]?.attributes.filter(
              (e: any) => e.key === 'amount'
            );

            const rewardValArr = valArrReward.map((item: any) => item.value);

            this.processDelegatorRewards(
              txData.fromAddress,
              rewardValArr,
              txData.timestamp
            );
            break;
          }

          case '/cosmos.staking.v1beta1.MsgUndelegate': {
            txData.type = TRANSACTION_TYPE.UNBOND_FUNDS;
            txData.toAddress = '/cosmos.staking.v1beta1.MsgUndelegate';

            const unbondEvent = events.find((e: any) => e.type === 'unbond');
            const amountAttr = unbondEvent?.attributes.find(
              (e: any) => e.key === 'amount'
            );
            val = (amountAttr?.value || '0').replace(/[^\d.]/g, '');
            break;
          }

          case '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward': {
            txData.type = TRANSACTION_TYPE.DELEGATOR_REWARD;
            txData.toAddress =
              '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward';

            const rewardEvent = events.find(
              (e: any) => e.type === 'withdraw_rewards'
            );
            const amountAttr = rewardEvent?.attributes.find(
              (e: any) => e.key === 'amount'
            );
            val = (amountAttr?.value || '0').replace(/[^\d.]/g, '');

            this.processDelegatorRewards(
              txData.fromAddress,
              [val.toString()],
              txData.timestamp
            );
            break;
          }

          case '/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission': {
            txData.type = TRANSACTION_TYPE.VALIDATOR_COMMISSION;
            txData.toAddress =
              '/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission';

            const commissionEvent = events.find(
              (e: any) => e.type === 'withdraw_commission'
            );
            const amountAttr = commissionEvent?.attributes.find(
              (e: any) => e.key === 'amount'
            );
            val = (amountAttr?.value || '0').replace(/[^\d.]/g, '');
            break;
          }

          // ✅ Other events same as before
          case '/cosmos.gov.v1beta1.MsgSubmitProposal':
            txData.type = TRANSACTION_TYPE.SUBMIT_PROPOSAL;
            txData.toAddress = '/cosmos.gov.v1.MsgSubmitProposal';
            break;

          case '/cosmos.gov.v1beta1.MsgVote':
            txData.type = TRANSACTION_TYPE.VOTING;
            txData.toAddress = '/cosmos.gov.v1.MsgVote';
            break;

          case '/cosmos.staking.v1beta1.MsgCreateValidator':
            txData.type = TRANSACTION_TYPE.CREATE_VALIDATOR;
            txData.toAddress = '/cosmos.staking.v1beta1.MsgCreateValidator';
            break;

          case '/cosmos.staking.v1beta1.MsgEditValidator':
            txData.type = TRANSACTION_TYPE.EDIT_VALIDATOR;
            txData.toAddress = '/cosmos.staking.v1beta1.MsgEditValidator';
            break;

          case '/cosmos.slashing.v1beta1.MsgUnjail':
            txData.type = TRANSACTION_TYPE.UNJAIL;
            txData.toAddress = '/cosmos.slashing.v1beta1.MsgUnjail';
            break;

          case '/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation': {
            txData.type = TRANSACTION_TYPE.CANCEL_UNBONDING_DELEGATION;
            txData.toAddress =
              '/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation';

            const filtered = events.filter(
              (e: any) => e.type === 'cancel_unbonding_delegation'
            );
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === 'amount'
            );
            val = (valArr[0].value || '0').replace(/[^\d.]/g, '');
            break;
          }
          default:
            txData.type = 'UNKNOWN';
            break;
        }

        let amount;

        if (val) {
          if (txData.type === TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE) {
            amount = noExponent(Number(retriveNumbers(val)) / 10 ** 6);
          } else {
            amount = noExponent(Number(retriveNumbers(val)) / 10 ** 18);
          }
        }
        txData.value = amount || '0';

        console.log('txData built', txData);
        const saveRes = await dbHelper.saveTransactions(txData);
        return !saveRes.error;
      }
      throw new Error(
        (fetchRes?.errorStack as string) || 'Error while fetching tx by hash'
      );
    } catch (error) {
      console.log('error while processing missed txs : ', error);
      return false;
    }
  }

  /**
   * Process and save error Blocks
   * @param blockNumber of type string
   * @returns
   */
  public async processErrorBlock(blockNumber: string) {
    try {
      // const url2 = `${environment.httpHost}/block?height=${blockNumber}`;
      const url2 = `${environment.evmHost}/cosmos/tx/v1beta1/txs/block/${blockNumber}`;
      const fetchRes = await fetchRequest(
        url2,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      const sessionData = await ExlorerHelper.getNodeEarning(blockNumber);

      if (sessionData) {
        const fullSessionData = {
          ...sessionData,
          blocknumber: fetchRes?.data.block?.header?.height,
          startAt: fetchRes?.data.block?.header?.time || null,
        };

        await dbHelper.saveSessionTransactions(fullSessionData);
      }

      if (!fetchRes.error) {
        const validatorOperatorAddress: string =
            await this.cosmjsInstace.getValoperFromBase64(
              fetchRes?.data.block?.header?.proposer_address
            ) ?? '';

          const miner =
            await this.cosmjsInstace.getDelegatorAddress(
              validatorOperatorAddress) ?? '';
        const dataToSave: IBlock = {
          miner,
          validatorOperatorAddress,
          blocknumber: Number(blockNumber),
          blockhash: `0x${fetchRes?.data?.block_id?.hash ?? ''}`,
          timestamp: fetchRes?.data.block?.header?.time ?? '',
          transactionCount: fetchRes?.data.block?.data?.txs?.length ?? 0,
        };

        for (
          let i = 0;
          i < fetchRes?.data?.block?.data?.txs.length || 0;
          i++
        ) {
          const element = fetchRes?.data?.block?.data?.txs[i];
          const hash = parseTx(element);
          rabbitmqService.inQueueData(QUEUE_NAME.DOWNTIME_TXS, hash.txHash);
        }

        await dbHelper.saveBlockData(dataToSave);
      } else {
        throw new Error(`Error while getting block by number ${blockNumber}`);
      }
      return true;
    } catch (error) {
      console.log('error while processing error blocks : ', blockNumber, error);
      return false;
    }
  }

  /**
   * Process and save missed bloks
   * @param blockData of type string
   * @returns
   */
  public async processMissedBlocks(blockData: IBlock) {
    try {
      if (blockData?.blocknumber && blockData?.blocknumber) {
        const saveRes = await dbHelper.saveBlockData(blockData);
        return !saveRes.error;
      }
      throw new Error('Error while saving missed blocks');
    } catch (error) {
      console.log('error while processing missed blocks : ', error);
      return false;
    }
  }

  /**
   * calculate downtime blocks
   */
  public async downTimeBlocks_1() {
    try {
      const lastBlockSaved = await RedisService.getString(
        REDIS_KEY.LATEST_BLOCK
      );
      const blockRes = await fetchRequest(
        `${environment.httpHost}/block`,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );
      const newBlock = blockRes?.data?.result?.block?.header?.height;

      if (newBlock && lastBlockSaved) {
        const diff = Number(newBlock) - Number(lastBlockSaved);
        if (diff > 1) {
          for (
            let i = Number(lastBlockSaved) + 1;
            i <= Number(newBlock) + 1;
            i += 1
          ) {
            const currentBlockRes = await fetchRequest(
              `${environment.httpHost}/block`,
              FETCH_METHODS.GET,
              HEADERS.DEFAULT,
              null
            );
            const currentBlock =
              currentBlockRes?.data?.result?.block?.header?.height;

            if (i <= currentBlock) {
              RabbitMqService.inQueueData(
                QUEUE_NAME.DOWNTIME_BLOCK,
                i.toString()
              );
            }
            // saving missed block to downtime block Queue
          }
        } else console.log('socket service is upto date.');
      }
    } catch (error) {
      console.log('error while getting downtimeblocks : ', error);
    }
  }

  public async processNullTimestamp(blockData: IBlock) {
    try {
      if (blockData?.blocknumber && blockData?.blocknumber) {
        const saveRes = await dbHelper.saveBlockData(blockData);
        return !saveRes.error;
      }
      throw new Error('Error while saving missed blocks');
    } catch (error) {
      console.log('error while processing missed blocks : ', error);
      return false;
    }
  }

  public async processLatestBlock_v2(message: any): Promise<boolean> {
    try {
      // Early validation
      const blockData = message?.result?.data?.value?.block;
      if (!blockData?.header) {
        console.warn('Invalid block data received', blockData);
        return false;
      }

      const { header, data: blockTxData } = blockData;
      const { height, time, proposer_address } = header;

      log('Processing blockNumber:', COLORS.cyan, height, COLORS.yellow);

      // Parallel fetch of validator and miner addresses
      const validatorOperatorAddress =
        await this.cosmjsInstace.tendermintValAddressToValoperAddress(
          proposer_address
        );

      const miner = await this.cosmjsInstace.getDelegatorAddress(
        validatorOperatorAddress
      );

      // Prepare block details
      const transactionCount = blockTxData?.txs?.length ?? 0;
      const blockDetails = {
        miner,
        validatorOperatorAddress,
        blocknumber: height,
        timestamp: time ?? '',
        transactionCount,
      };

      // Parallel execution of independent operations
      const [analytics, sessionData, blockHashResponse] =
        await Promise.allSettled([
          dbHelper.getAnalytics(),
          ExlorerHelper.getNodeEarning(height),
          fetchRequest(
            `${environment.httpHost}/block?height=${height}`,
            FETCH_METHODS.GET,
            HEADERS.DEFAULT,
            null
          ),
        ]);

      // Emit transaction summary
      const totalTx = await prisma.transactions.count();
      SocketEventEmitter.emitMessage(SOCKET_EVENT.TX_SUMMARY, {
        tpb: transactionCount,
        totalTx,
      });

      // Emit analytics if successful
      if (analytics.status === 'fulfilled') {
        SocketEventEmitter.emitMessage(SOCKET_EVENT.ANALYTICS, analytics.value);
      }

      // Save session data if available
      if (sessionData.status === 'fulfilled' && sessionData.value) {
        const fullSessionData = {
          ...sessionData.value,
          blocknumber: height,
          startAt: time || null,
        };
        await dbHelper.saveSessionTransactions(fullSessionData);
      }

      // Process block hash and save block data
      if (
        blockHashResponse.status === 'fulfilled' &&
        !blockHashResponse.value.error
      ) {
        const blockHash = `0x${blockHashResponse.value.data?.result?.block_id?.hash ?? ''}`;
        const blockDataToSave: IBlock = {
          ...blockDetails,
          blockhash: blockHash,
        };

        // Emit latest block event
        SocketEventEmitter.emitMessage(
          SOCKET_EVENT.NATIVE_LATEST_BLOCK,
          blockDataToSave
        );

        // Save block data
        const saveRes = await dbHelper.saveBlockData(blockDataToSave);

        if (saveRes?.error) {
          throw new Error(`Error saving block data: ${saveRes.error}`);
        }

        // Update Redis and validator counts in parallel
        await Promise.all([
          this.updateBlockCount(),
          dbHelper.upsertValidatorsCount(
            blockDataToSave.validatorOperatorAddress,
            blockDataToSave.miner
          ),
          RedisService.setString(REDIS_KEY.LATEST_BLOCK, height.toString()),
        ]);
      } else {
        const errorMsg =
          blockHashResponse.status === 'rejected'
            ? blockHashResponse.reason
            : blockHashResponse.value?.errorStack;
        console.error('Failed to fetch block hash:', errorMsg);
        return false;
      }

      return true;
    } catch (error) {
      console.error(
        'Error processing block:',
        message?.result?.data?.value?.block?.header?.height,
        error
      );
      return false;
    }
  }

  /**
   * Helper method to handle block count updates atomically
   */
  private async updateBlockCount(): Promise<void> {
    try {
      const blockCount = await RedisService.getString(
        REDIS_KEY.NATIVE_BLOCKS_COUNT
      );

      if (!blockCount) {
        // Initialize from database
        const dbCount = await prisma.blocks.count();
        await RedisService.setString(
          REDIS_KEY.NATIVE_BLOCKS_COUNT,
          dbCount.toString()
        );
      } else {
        // Increment existing count
        await RedisService.setString(
          REDIS_KEY.NATIVE_BLOCKS_COUNT,
          (Number(blockCount) + 1).toString()
        );
      }
    } catch (error) {
      console.error('Failed to update block count in Redis:', error);
      // Non-critical error - don't throw
    }
  }

  public async updateFromAddress(
    txHash: string,
    fromAddress: string
  ): Promise<void> {
    try {
      console.log(txHash, 'txHash--io');

      const url = `${environment.httpHost}/tx?hash=${txHash}`;
      const hashResult = await fetchRequest(
        url,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      const events = hashResult?.data?.result?.tx_result?.events;

      const actionSender = getActionAndSender(events);
      const getSender = actionSender?.sender;

      if (getSender) {
        if (getSender === fromAddress) {
          await prisma.transactions.update({
            where: {
              txhash: txHash,
            },
            data: {
              need_fix: false,
            },
          });
          console.log('same fromAddressIX');

          
        } else {
          await prisma.transactions.update({
            where: {
              txhash: txHash,
            },
            data: {
              need_fix: false,
              fromAddress: getSender,
            },
          });
          console.log('different fromAddressIX');

          
        }
      }

      
    } catch (error) {
      console.error('Failed to update updateFromAddress', txHash, error);
      // Non-critical error - don't throw
    }
  }

  public async processTxs(message: any) {
    try {
      const events = message?.result?.events;
      const data = message?.result?.data;
      if (!data || !events) return true;

      const code = data?.value?.TxResult?.result?.code;
      const status = !code
        ? TRANSACTION_STATUS.SUCCESS
        : TRANSACTION_STATUS.FAILED;

      // --- helpers ---
      const getEvt = (key: string, idx = 0) =>
        events?.[key] ? events[key][idx] : undefined;

      const firstEvt = (key: string) => getEvt(key, 0);
      const lastEvt = (key: string) =>
        events?.[key] ? events[key][events[key].length - 1] : undefined;

      const joinIfArray = (arr: any[] | undefined) =>
        Array.isArray(arr) ? arr.join(',') : (arr ?? '');

      const parseCoinsToHuman = (
        raw: any,
        denomDecimals = 18,
        isIbc = false
      ) => {
        if (!raw) return 0;
        const nums = retriveNumbers(String(raw));
        const digits = isIbc ? 6 : denomDecimals;
        return noExponent(Number(nums) / 10 ** digits);
      };

      // tx basic info
      const txEvent = lastEvt('message.action') ?? '';
      const logs: string = data?.value?.TxResult?.result?.log ?? '';
      const hash = getEvt('tx.hash') ?? '';
      console.log('--TXhash--: ', hash);

      const firstIndexAddress: string = firstEvt('message.sender') ?? '';
      const actionSender = getActionAndSender(events);
      let fromAddress = actionSender?.sender ?? firstIndexAddress;
      let toAddress = '';
      let contractAddress = '';
      let type: string | undefined = '';
      let val: any = 0;
      let method = '';
      let timeStamp = '';

      // fetch full tx for timestamp (if available)
      try {
        const url = `${environment.evmHost}/cosmos/tx/v1beta1/txs/${hash}`;
        const fetchRes = await fetchRequest(
          url,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null
        );
        if (!fetchRes.error) {
          timeStamp = String(fetchRes?.data?.tx_response?.timestamp ?? '');
        }
      } catch (err) {
        console.log('tx fetch error', err);
      }

      // --- Handle common actions + keep it compact ---
      switch (txEvent) {
        // BANK: single send
        case '/cosmos.bank.v1beta1.MsgSend':
          type = TRANSACTION_TYPE.COIN_TRANSFER;
          val = lastEvt('transfer.amount') ?? firstEvt('transfer.amount') ?? 0;
          toAddress =
            lastEvt('transfer.recipient') ??
            firstEvt('transfer.recipient') ??
            '';
          break;

        // BANK: multisend (multiple inputs / outputs)
        case '/cosmos.bank.v1beta1.MsgMultiSend': {
  type = TRANSACTION_TYPE.COIN_TRANSFER;

  const inputs =
    events['multi_send.input.address'] ??
    events['input.address'] ??
    data?.value?.TxResult?.tx?.body?.messages?.[0]?.inputs ??
    [];

  const outputs =
    events['multi_send.output.address'] ??
    events['output.address'] ??
    data?.value?.TxResult?.tx?.body?.messages?.[0]?.outputs ??
    [];

  const froms = Array.isArray(inputs)
    ? inputs
        .map((i: any) => (typeof i === 'string' ? i : i.address))
        .filter(Boolean)
    : [];

  const tos = Array.isArray(outputs)
    ? outputs
        .map((o: any) => (typeof o === 'string' ? o : o.address))
        .filter(Boolean)
    : [];

  const outputAmountsEvt =
    events['transfer.amount'] ??
    events['multi_send.output.coins'] ??
    [];

  let amountsArr: any[] = [];

  if (Array.isArray(outputAmountsEvt) && outputAmountsEvt.length) {
    amountsArr = outputAmountsEvt;
  } else if (Array.isArray(outputs)) {
    amountsArr = outputs.map((o: any) =>
      o?.coins?.[0]?.amount ?? '0'
    );
  }

  amountsArr = amountsArr.map((a: any) => String(a ?? '0'));

  fromAddress = joinIfArray(froms) || fromAddress;
  toAddress = joinIfArray(tos);
  val = amountsArr.join(',');

  break;
}


        // STAKING
        case '/cosmos.staking.v1beta1.MsgDelegate':
          type =
            events['message.sender']?.length === 1
              ? TRANSACTION_TYPE.CREATE_DELEGATOR
              : TRANSACTION_TYPE.BOND_MORE;
          val = firstEvt('delegate.amount') ?? 0;
          toAddress = '/cosmos.staking.v1beta1.MsgDelegate';
          if (events['withdraw_rewards.amount']) {
            this.processDelegatorRewards(
              fromAddress,
              events['withdraw_rewards.amount'],
              timeStamp
            );
          }
          break;

        case '/cosmos.staking.v1beta1.MsgUndelegate':
          type = TRANSACTION_TYPE.UNBOND_FUNDS;
          val = firstEvt('unbond.amount') ?? 0;
          toAddress = '/cosmos.staking.v1beta1.MsgUndelegate';
          if (events['withdraw_rewards.amount']) {
            this.processDelegatorRewards(
              fromAddress,
              events['withdraw_rewards.amount'],
              timeStamp
            );
          }
          break;

        case '/cosmos.staking.v1beta1.MsgBeginRedelegate':
          type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
          val = firstEvt('redelegate.amount') ?? 0;
          toAddress = '/cosmos.staking.v1beta1.MsgBeginRedelegate';
          if (events['withdraw_rewards.amount']) {
            this.processDelegatorRewards(
              fromAddress,
              events['withdraw_rewards.amount'],
              timeStamp
            );
          }
          break;

        case '/cosmos.staking.v1beta1.MsgCreateValidator':
          type = TRANSACTION_TYPE.CREATE_VALIDATOR;
          val = firstEvt('create_validator.amount') ?? 0;
          toAddress = '/cosmos.staking.v1beta1.MsgCreateValidator';
          break;

        // GOVERNANCE
        case '/cosmos.gov.v1beta1.MsgSubmitProposal':
          type = TRANSACTION_TYPE.SUBMIT_PROPOSAL;
          val =
            firstEvt('proposal.deposit') ?? firstEvt('transfer.amount') ?? 0;
          toAddress = '/cosmos.gov.v1beta1.MsgSubmitProposal';
          break;

        case '/cosmos.gov.v1beta1.MsgVote':
          type = TRANSACTION_TYPE.VOTING;
          toAddress = '/cosmos.gov.v1beta1.MsgVote';
          val = 0;
          break;

        // IBC transfers (initiate)
        case '/ibc.applications.transfer.v1.MsgTransfer':
          type = TRANSACTION_TYPE.IBC_TRANSFER_INITIATE;
          val = firstEvt('transfer.amount') ?? 0;
          toAddress = firstEvt('ibc_transfer.receiver') ?? '';
          break;

        // IBC composite events (update client leads to inner channel actions)
        case '/ibc.core.client.v1.MsgUpdateClient': {
          const inner = events['message.action']?.[1] ?? '';
          if (inner === '/ibc.core.channel.v1.MsgRecvPacket') {
            type = TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE;
            val = firstEvt('coin_spent.amount') ?? 0;
            toAddress = firstEvt('fungible_token_packet.receiver') ?? '';
            fromAddress =
              firstEvt('fungible_token_packet.sender') ?? fromAddress;
          } else if (
            inner === '/ibc.core.channel.v1.MsgAcknowledgement' ||
            inner === '/ibc.core.channel.v1.MsgAcknowledgement'
          ) {
            type = TRANSACTION_TYPE.IBC_TRANSFER_CONFIRM;
            val = firstEvt('fungible_token_packet.amount') ?? 0;
            toAddress = firstEvt('fungible_token_packet.receiver') ?? '';
            fromAddress =
              firstEvt('fungible_token_packet.sender') ?? fromAddress;
          } else {
            type = 'UNKNOWN';
          }
          break;
        }

        // COSMWASM
        case '/cosmwasm.wasm.v1.MsgInstantiateContract':
          type = TRANSACTION_TYPE.CONTRACT_CREATION;
          contractAddress = firstEvt('instantiate._contract_address') ?? '';
          break;

        case '/cosmwasm.wasm.v1.MsgExecuteContract':
          type = TRANSACTION_TYPE.CONTRACT_EXECUTE;
          contractAddress = firstEvt('execute._contract_address') ?? '';
          val = firstEvt('wasm.amount') ?? 0;
          method = firstEvt('wasm.action') ?? '';
          break;

        case '/cosmwasm.wasm.v1.MsgStoreCode':
          type = TRANSACTION_TYPE.CONTRACT_UPLOAD;
          break;

        // AUTHZ (exec)
        case '/cosmos.authz.v1beta1.MsgExec':
          type = TRANSACTION_TYPE.RESTAKING_EXECUTION;
          val = firstEvt('transfer.amount') ?? 0;
          toAddress = '/cosmos.authz.v1beta1.MsgExec';
          break;

        // EVM (ethermint)
        case '/ethermint.evm.v1.MsgEthereumTx':
          type = TRANSACTION_TYPE.COIN_RECEIVE;
          val =
            firstEvt('ethereum_tx.amount') ??
            firstEvt('coin_received.amount') ??
            0;
          toAddress = lastEvt('transfer.recipient') ?? '';
          break;

        // Custom DVPN / qubetics sample handlers (kept as-is)
        case '/qubetics.subscription.v3.MsgStartSubscriptionRequest':
          type = TRANSACTION_TYPE.START_SUBSCRIPTION;
          val = firstEvt('qubetics.subscription.v3.EventPay.payment') ?? 0;
          // other fields handled later below when saving session
          break;

        case '/qubetics.subscription.v3.MsgStartSessionRequest':
          type = TRANSACTION_TYPE.START_REQUEST;
          break;

        case '/qubetics.session.v3.MsgCancelSessionRequest':
          type = TRANSACTION_TYPE.END_REQUEST;
          break;

        case '/qubetics.subscription.v3.MsgCancelSubscriptionRequest':
          type = TRANSACTION_TYPE.END_SUBSCRIPTION;
          break;

        // default fallback: preserve txEvent as type so we won't lose info
        default:
          type = txEvent || 'UNKNOWN';
          break;
      }

      // --- normalize numeric amounts into human-readable 'amount' variable ---
      let amount: any;
      if (val) {
        // IBC transfers usually have 6 decimals, wasm/other use different decimals.
        if (
          [
            TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE,
            TRANSACTION_TYPE.IBC_TRANSFER_INITIATE,
          ].includes(type as any)
        ) {
          amount = parseCoinsToHuman(val, 6, true);
        } else if (
          type === TRANSACTION_TYPE.START_SUBSCRIPTION ||
          type === TRANSACTION_TYPE.START_REQUEST
        ) {
          amount = parseTics(String(val));
        } else {
          // default assume 18 decimals
          amount = parseCoinsToHuman(val, 18, false);
        }
      } else {
        amount = val ?? 0;
      }
      console.log('amount---', amount);

      // tx fee
      const txFeeData = getEvt('tx.fee')?.match(/[\d.]+/g)?.[0] ?? 0;
      const txFee = txFeeData
        ? (Number(txFeeData) / 10 ** 18).toString()
        : String(txFeeData);

      // build txData similar to before
      const txData = {
        type: getType(<string>type),
        status,
        value: amount ? String(amount) : '0',
        timestamp: timeStamp || '',
        txString: data?.value?.TxResult?.tx ?? '',
        txhash: `0x${hash ?? ''}`,
        toAddress: cleanEscapedString(toAddress),
        blocknumber: Number(data?.value?.TxResult?.height ?? 0),
        fromAddress: cleanEscapedString(fromAddress),
        nodeAddress: cleanEscapedString(getEvt('node.address') ?? ''),
        txFee: String(txFee),
        gasWanted: Number(data?.value?.TxResult?.result?.gas_wanted ?? 0),
        gasUsed: Number(data?.value?.TxResult?.result?.gas_used ?? 0),
        contractAddress: contractAddress ?? '',
      };

      // emit socket + save
      socketEventsHelper.emitMessage(
        SOCKET_EVENT.NATIVE_LATEST_TRANSACTIONS,
        txData
      );
      console.log('txData : ', txData);

      await dbHelper.saveTransactions(txData);

      // Additional per-type handlers (voting, contract, sessions) kept concise:
      if (txData.type === TRANSACTION_TYPE.VOTING) {
        let answer = firstEvt('proposal_vote.option')
          ? firstEvt('proposal_vote.option').split(' ')[0].split(':')[1]
          : '';
        switch (answer) {
          case RES_MSG.VOTE_OPTION_YES:
            answer = RES_MSG.YES;
            break;
          case RES_MSG.VOTE_OPTION_NO:
            answer = RES_MSG.NO;
            break;
          case RES_MSG.VOTE_OPTION_NO_WITH_VETO:
            answer = RES_MSG.VETO;
            break;
          case RES_MSG.VOTE_OPTION_ABSTAIN:
            answer = RES_MSG.ABSTAIN;
            break;
          default:
            break;
        }
        const voters: IVoters = {
          proposalId: firstEvt('proposal_vote.proposal_id') ?? '',
          answer,
          txHash: txData.txhash,
          blockNumber: firstEvt('tx.height') ?? '',
          voter: firstEvt('message.sender') ?? '',
        };
        await dbHelper.saveVoters(voters);
      }

      // Session / subscription flows (same as before but compact)
      if (
        [
          TRANSACTION_TYPE.START_SUBSCRIPTION,
          TRANSACTION_TYPE.START_REQUEST,
          TRANSACTION_TYPE.END_REQUEST,
          TRANSACTION_TYPE.END_SUBSCRIPTION,
        ].includes(txData.type as any)
      ) {
        const stackingRewardValue =
          firstEvt('qubetics.subscription.v3.EventPay.staking_reward') ?? 0;
        const provAddress =
          firstEvt('qubetics.subscription.v3.EventCreate.prov_address') ??
          firstEvt('qubetics.session.v3.EventUpdateStatus.prov_address') ??
          '';
        const accAddress =
          firstEvt('qubetics.subscription.v3.EventCreate.acc_address') ??
          firstEvt('qubetics.session.v3.EventCreateSession.acc_address') ??
          '';
        const nodeAddress =
          firstEvt(
            'qubetics.subscription.v3.EventCreateSession.node_address'
          ) ??
          firstEvt('qubetics.session.v3.EventUpdateStatus.node_address') ??
          '';
        const subscriptionId =
          firstEvt('qubetics.subscription.v3.EventCreate.id') ??
          firstEvt(
            'qubetics.subscription.v3.EventCreateSession.subscription_id'
          ) ??
          firstEvt('qubetics.session.v3.EventUpdateStatus.id') ??
          '';
        const planIdRaw =
          firstEvt('qubetics.subscription.v3.EventCreate.plan_id') ??
          firstEvt('qubetics.subscription.v3.EventUpdate.plan_id') ??
          '';
        const planId = planIdRaw
          ? Number(String(planIdRaw).replace(/"/g, ''))
          : undefined;

        let inactiveAt = null;
        if (planId) {
          try {
            const planDetailsUrl = `${environment.evmHost}/qubetics/plan/v3/plans/${planId}`;
            const planResponse = await fetchRequest(
              planDetailsUrl,
              FETCH_METHODS.GET,
              HEADERS.DEFAULT,
              null
            );
            if (!planResponse.error) {
              inactiveAt = addHours(timeStamp, planResponse?.data?.plan?.hours);
            }
          } catch (err) {
            console.log(err, 'plan fetch error');
          }
        }

        const sessionData = {
          logs,
          txhash: txData.txhash,
          subscriptionId: subscriptionId ?? '',
          planId: planId?.toString() ?? '',
          sessionId:
            firstEvt('qubetics.subscription.v3.EventCreateSession.id') ?? '',
          status:
            firstEvt('qubetics.session.v3.EventUpdateStatus.status') ?? '',
          type: txEvent,
          provAddress: cleanEscapedString(provAddress),
          accAddress: cleanEscapedString(accAddress),
          nodeAddress: cleanEscapedString(nodeAddress),
          price: amount ? amount?.toString() : '',
          stackingReward: stackingRewardValue
            ? stackingRewardValue?.toString()
            : '',
          blocknumber: txData.blocknumber,
          startAt: txData.timestamp || null,
          inactiveAt: inactiveAt || null,
        };

        await dbHelper.saveSessionTransactions(sessionData);
      }

      // Contract creation/execution flows (kept same logic)
      if (
        txData.type === TRANSACTION_TYPE.CONTRACT_CREATION &&
        txData.contractAddress
      ) {
        let contractType = await this.cosmjsInstace.getContractType(
          txData.contractAddress
        );
        contractType = contractType ?? 'UNKNOWN';

        const contractDetails: IContracts = {
          contractName: '',
          txHash: txData.txhash,
          blockNumber: firstEvt('tx.height') ?? '',
          address: txData.contractAddress,
          contractType,
          timestamp: txData.timestamp ?? '',
          creator: firstEvt('message.sender') ?? '',
        };

        const contractName = await this.cosmjsInstace.getContractName(
          contractDetails.address
        );
        if (contractName !== null) contractDetails.contractName = contractName;

        await dbHelper.saveContractDeployTx(contractDetails);

        const tokenInfo = await this.cosmjsInstace.getTokenInfo(
          contractDetails.address
        );
        if (tokenInfo !== null) {
          const {
            name: tokenName,
            symbol: tokenSymbol,
            decimals,
            total_supply: totalSupply,
          } = tokenInfo;
          const tokens: ITokens = {
            tokenName,
            tokenSymbol,
            creator: contractDetails.creator,
            type: contractDetails.contractType,
            decimal: noExponent(decimals),
            blockNumber: contractDetails.blockNumber,
            totalSupply: noExponent(totalSupply),
            contractAddress: contractDetails.address,
          };
          await dbHelper.saveToken(tokens);
        }
      } else if (txData.type === TRANSACTION_TYPE.CONTRACT_EXECUTE) {
        const contractLogData: IContractLogs = {
          logs,
          method,
          txFee: txData.txFee,
          txHash: txData.txhash,
          status: txData.status,
          timestamp: txData.timestamp ?? '',
          contractAddress: txData.contractAddress,
          blockNumber: Number(txData.blocknumber),
        };
        await dbHelper.saveContractLogs(contractLogData);
        await dbHelper.updateContractExec(txData.contractAddress);
        const account = await this.cosmjsInstace.getAccountInfo(
          txData.contractAddress
        );
        await this.cosmjsInstace.getTokenBalances(
          txData.contractAddress,
          account
        );
      }

      return true;
    } catch (error) {
      console.log('error while processing transactions ', error);
      return false;
    }
  }

  public async processLatestBlock(message: any): Promise<boolean> {
    try {
      /* ===================== VALIDATION ===================== */
      const value = message?.result?.data?.value;
      const block = value?.block;
      if (!block?.header) {
        console.warn('Invalid WS block payload', {
          header: block?.header,
          // blockId,
        });
        return false;
      }

      const { header, data: blockTxData } = block;
      const { height, time, proposer_address } = header;
      log('Processing blockNumber:', COLORS.cyan, height, COLORS.yellow);

      /* ===================== ADDRESS RESOLUTION ===================== */
      const validatorOperatorAddress =
        await this.cosmjsInstace.tendermintValAddressToValoperAddress(
          proposer_address
        );

      const miner = await this.cosmjsInstace.getDelegatorAddress(
        validatorOperatorAddress
      );

      /* ===================== BLOCK PREP ===================== */
      const transactionCount = blockTxData?.txs?.length ?? 0;

      const blockDetails = {
        miner,
        validatorOperatorAddress,
        blocknumber: height,
        timestamp: time ?? '',
        transactionCount,
      };

      // const blockHash = `0x${blockId.hash}`;

      /* ===================== PARALLEL READ OPS ===================== */
      const [analyticsRes, sessionRes] = await Promise.allSettled([
        dbHelper.getAnalytics(),
        ExlorerHelper.getNodeEarning(height),
      ]);

      /* ===================== SOCKET EMITS ===================== */
      const totalTx = await prisma.transactions.count();

      SocketEventEmitter.emitMessage(SOCKET_EVENT.TX_SUMMARY, {
        tpb: transactionCount,
        totalTx,
      });

      if (analyticsRes.status === 'fulfilled') {
        SocketEventEmitter.emitMessage(
          SOCKET_EVENT.ANALYTICS,
          analyticsRes.value
        );
      }

      /* ===================== SESSION SAVE ===================== */
      if (sessionRes.status === 'fulfilled' && sessionRes.value) {
        await dbHelper.saveSessionTransactions({
          ...sessionRes.value,
          blocknumber: height,
          startAt: time || null,
        });
      }

      /* ===================== BLOCK SAVE ===================== */
      const blockDataToSave: IBlock = {
        ...blockDetails,
        // blockhash: blockHash,
      };

      SocketEventEmitter.emitMessage(
        SOCKET_EVENT.NATIVE_LATEST_BLOCK,
        blockDataToSave
      );

      const saveRes = await dbHelper.saveBlockData(blockDataToSave);

      if (saveRes?.error) {
        throw new Error(`Error saving block data: ${saveRes.error}`);
      }

      /* ===================== PARALLEL WRITES ===================== */
      await Promise.all([
        this.updateBlockCount(),
        dbHelper.upsertValidatorsCount(
          blockDataToSave.validatorOperatorAddress,
          blockDataToSave.miner
        ),
        RedisService.setString(REDIS_KEY.LATEST_BLOCK, height.toString()),
      ]);

      return true;
    } catch (error) {
      console.error(
        'Error processing block:',
        message?.result?.data?.value?.block?.header?.height,
        error
      );
      return false;
    }
  }

  public async downTimeBlocks() {
  try {
    const lastBlockSaved = await RedisService.getString(
      REDIS_KEY.LATEST_BLOCK
    );

    if (!lastBlockSaved) {
      console.log('No last saved block found.');
      return;
    }

    const blockRes = await fetchRequest(
      `${environment.httpHost}/block`,
      FETCH_METHODS.GET,
      HEADERS.DEFAULT,
      null
    );

    const latestBlock =
      blockRes?.data?.result?.block?.header?.height;

    if (!latestBlock) {
      console.log('Unable to fetch latest block.');
      return;
    }

    const lastSaved = Number(lastBlockSaved);
    const current = Number(latestBlock);
    const diff = current - lastSaved;

    if (diff <= 1) {
      console.log('Socket service is up to date.');
      return;
    }

    // Enqueue missed blocks
    for (let i = lastSaved + 1; i <= current; i++) {
      RabbitMqService.inQueueData(
        QUEUE_NAME.DOWNTIME_BLOCK,
        i.toString()
      );
    }

    // Update Redis after processing
    await RedisService.setString(
      REDIS_KEY.LATEST_BLOCK,
      current.toString()
    );

    console.log(`Enqueued ${diff - 1} downtime blocks.`);
  } catch (error) {
    console.error('Error while getting downtime blocks:', error);
  }
}

}
export default new ProcessHelper();
