import { fromBase64, toHex } from '@cosmjss/encoding';
import { BigNumber } from 'bignumber.js';
import { v4 } from 'uuid';
import {
  log,
  fetchRequest,
  noExponent,
  retriveNumbers,
  calculateBurnedAmount,
  cleanEscapedString,
  getType,
  getActionAndSender,
  parseTx,
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
  STATIC_BURNED_AMOUNT,
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
      // const [analyticsRes, sessionRes] = await Promise.allSettled([
      //   dbHelper.getAnalytics(),
      //   ExlorerHelper.getNodeEarning(height),
      // ]);

      /* ===================== SOCKET EMITS ===================== */
      const totalTx = await prisma.transactions.count();

      SocketEventEmitter.emitMessage(SOCKET_EVENT.TX_SUMMARY, {
        tpb: transactionCount,
        totalTx,
      });

      // if (analyticsRes.status === "fulfilled") {
      //   SocketEventEmitter.emitMessage(
      //     SOCKET_EVENT.ANALYTICS,
      //     analyticsRes.value
      //   );
      // }

      /* ===================== SESSION SAVE ===================== */
      // if (sessionRes.status === "fulfilled" && sessionRes.value) {
      //   await dbHelper.saveSessionTransactions({
      //     ...sessionRes.value,
      //     blocknumber: height,
      //     startAt: time || null,
      //   });
      // }

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

  async processDelegatorRewards(
    address: string,
    rewardEvent: string[],
    timeStamp: string
  ) {
    let rewardAmount = BigNumber(0);

    for (let i = 0; i < rewardEvent?.length; i++) {
      const element = rewardEvent[i];
      let amountArr: any[] = [];

      if (
        typeof element === 'string' &&
        element.includes(environment.symbol.toLowerCase())
      ) {
        amountArr = element.split('t');
      }
      rewardAmount = rewardAmount.plus(amountArr[0] || 0);
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

        console.log('Updating new Reward in table and redis ', newRewardToSet);

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
  }

  /**
   * process latest transactions
   * @param message
   * @returns
   */
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
          // inputs / outputs found in events or in message body (if available)
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

          // if events provide addresses as string array, use them; else parse message body
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

          // amounts: try to use transfer.amount events, else derive from outputs
          const outputAmountsEvt =
            events['transfer.amount'] ??
            events['multi_send.output.coins'] ??
            [];
          let amountsArr: string[] = [];

if (Array.isArray(outputAmountsEvt) && outputAmountsEvt.length) {
  amountsArr = outputAmountsEvt;
} else if (Array.isArray(outputs)) {
  amountsArr = outputs.map((o: any) => (o.coins ? o.coins[0]?.amount : undefined));
} else {
  amountsArr = [];
}

// fallback sanitize
amountsArr = amountsArr.map((a: any) => (a ? String(a) : '0'));


          // fallback sanitize
          // amountsArr = amountsArr.map((a: any) => (a ? String(a) : '0'));

          fromAddress = joinIfArray(froms) || fromAddress;
          toAddress = joinIfArray(tos);
          val = amountsArr.join(','); // store comma-joined amounts (simple migration)
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
        } else {
          // default assume 18 decimals
          amount = parseCoinsToHuman(val, 18, false);
        }
      } else {
        amount = val ?? 0;
      }

      // tx fee
      const txFeeData = getEvt('tx.fee')?.match(/[\d.]+/g)?.[0] ?? 0;
      const txFee = txFeeData
        ? (Number(txFeeData) / 10 ** 18).toString()
        : String(txFeeData);

      // build txData similar to before
      const txData = {
        type: getType(<string>type) ?? '',
        status,
        value: amount ? String(amount) : '0',
        timestamp: timeStamp || '',
        txString: data?.value?.TxResult?.tx ?? '',
        txhash: `0x${hash ?? ''}`,
        toAddress: cleanEscapedString(toAddress),
        blocknumber: Number(data?.value?.TxResult?.height ?? 0),
        fromAddress: cleanEscapedString(fromAddress),
        // nodeAddress: cleanEscapedString(getEvt("node.address") ?? ""),
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
      if (fetchRes?.data?.result && !fetchRes.error) {
        console.log(
          'tx Data inside missedTx : ',
          JSON.stringify(fetchRes?.data)
        );

        const result = fetchRes?.data?.result;
        const events = result?.tx_result?.events;
        const code = result?.tx_result?.code;
        const status = !code
          ? TRANSACTION_STATUS.SUCCESS
          : TRANSACTION_STATUS.FAILED;
        const burnedFee = (
          Number(fetchRes.data.result?.tx_result?.gas_used) * 0.2
        ).toFixed(11);

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
          timestamp: '',
          contractAddress: '',
          burnedFee: Number(burnedFee),
        };

        const filteredEvents = events.filter(
          (event: any) =>
            event.type === 'message' &&
            event?.attributes.some((attr: any) => attr.key === 'action')
        );

        const eventObj = filteredEvents[0]?.attributes.find(
          (i: any) => i.key === 'action'
        );
        const txEvent = eventObj?.value || '';

        switch (txEvent) {
          case '/cosmos.bank.v1beta1.MsgSend':
            txData.type = TRANSACTION_TYPE.COIN_TRANSFER;
            break;
          case '/cosmos.staking.v1beta1.MsgDelegate':
            txData.type = TRANSACTION_TYPE.BOND_MORE;
            break;

          case '/cosmos.staking.v1beta1.MsgUndelegate':
            txData.type = TRANSACTION_TYPE.UNBOND_FUNDS;
            break;

          case '/cosmwasm.wasm.v1.MsgInstantiateContract':
            txData.type = TRANSACTION_TYPE.CONTRACT_CREATION;
            break;

          case '/cosmwasm.wasm.v1.MsgExecuteContract':
            txData.type = TRANSACTION_TYPE.CONTRACT_EXECUTE;
            break;

          default:
            break;
        }

        const transferEvent = events.find((i: any) => i.type === 'transfer');

        transferEvent?.attributes?.forEach((i: any) => {
          switch (i.key) {
            case 'recipient':
              txData.toAddress = i?.value || '';
              break;

            case 'sender':
              txData.fromAddress = i?.value || '';
              break;

            case 'amount': {
              const val = i?.value ?? 0;
              const amount = val
                ? noExponent(Number(retriveNumbers(val)) / 10 ** 18)
                : val;
              txData.value = amount;
              break;
            }

            default:
              break;
          }
        });

        const url2 = `${environment.httpHost}/block?height=${txData.blocknumber}`;
        const fetchRes2 = await fetchRequest(
          url2,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null
        );
        if (!fetchRes2.error) {
          txData.timestamp = fetchRes2?.data.result.block?.header?.time ?? '';
        }

        console.log('start missed tx+++++++++++++', Date.now());
        const saveRes = await dbHelper.saveTransactions(txData);
        console.log('end missed tx+++++++++++++', Date.now());
        console.log('txData: ', saveRes, txData);
        return !saveRes.error;
      }
      throw new Error('Error while getting TX by hash ');
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

      const blockhash= toHex(fromBase64(fetchRes?.data?.block_id?.hash)).toUpperCase();

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
          blockhash: `0x${blockhash ?? ''}`,
          timestamp: fetchRes?.data.block?.header?.time ?? '',
          transactionCount: fetchRes?.data.block?.data?.txs?.length ?? 0,
        };

        // for (
        //   let i = 0;
        //   i < fetchRes?.data?.block?.data?.txs.length || 0;
        //   i++
        // ) {
        //   const element = fetchRes?.data?.block?.data?.txs[i];
        //   const hash = parseTx(element);
        //   rabbitmqService.inQueueData(QUEUE_NAME.DOWNTIME_BLOCK, hash.txHash);
        // }

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
      // console.log('dataToSave:  missed block :  ', blockData);

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

  public async processNullTimestamp(blockData: IBlock) {
    try {
      // console.log('dataToSave:  missed block :  ', blockData);

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
}
export default new ProcessHelper();
