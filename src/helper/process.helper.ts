import { BigNumber } from "bignumber.js";
import { v4 } from "uuid";
import {
  log,
  fetchRequest,
  noExponent,
  retriveNumbers,
  calculateBurnedAmount,
  cleanEscapedString,
  getType,
  parseTics,
  getActionAndSender,
  addHours,
} from "../Utility/common";
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
} from "../constant";
import {
  IBlock,
  IContractLogs,
  IContracts,
  ITokens,
  ITransaction,
  IVoters,
} from "../interface";
import { prisma } from "../lib/db";
import { RabbitMqService, RedisService, SocketEventEmitter } from "../service";
import redisHelper from "../service/redis.helper";
import socketEventsHelper from "../service/socketEvents.helper";
import { CosmJsRpcMethods } from "./cosmsClient.helper";
import dbHelper from "./db.helper";
import ExlorerHelper from "./db.helper";
import { CustomWebSocket } from "./ws.helper";

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
      jsonrpc: "2.0",
      method: "subscribe",
      id: v4().toString(),
      params: {
        query: "tm.event='NewBlock'",
      },
    };
    const blockSocket = new CustomWebSocket(
      `${environment.wsUrl}/websocket`,
      QUEUE_NAME.LATEST_BLOCKS,
      blockWsQuery,
    );
    blockSocket.connect();
  }

  /**
   * Subscribe to latest Transaction
   */
  public subscribeLatestTxs() {
    const txWsQuery = {
      jsonrpc: "2.0",
      method: "subscribe",
      id: v4().toString(),
      params: {
        query: "tm.event='Tx'",
      },
    };
    const txSocket = new CustomWebSocket(
      `${environment.wsUrl}/websocket`,
      QUEUE_NAME.LATEST_TXS,
      txWsQuery,
    );
    txSocket.connect();
  }

  /**
   * process latest block (from v2stable - optimized version)
   * @param message
   * @returns
   */
  public async processLatestBlock(message: any): Promise<boolean> {
    try {
      /* ===================== VALIDATION ===================== */
      const value = message?.result?.data?.value;
      const block = value?.block;
      if (!block?.header) {
        console.warn("Invalid WS block payload", {
          header: block?.header,
          // blockId,
        });
        return false;
      }

      const { header, data: blockTxData } = block;
      const { height, time, proposer_address } = header;
      log("Processing blockNumber:", COLORS.cyan, height, COLORS.yellow);

      /* ===================== ADDRESS RESOLUTION ===================== */
      const validatorOperatorAddress =
        await this.cosmjsInstace.tendermintValAddressToValoperAddress(
          proposer_address,
        );

      const miner = await this.cosmjsInstace.getDelegatorAddress(
        validatorOperatorAddress,
      );

      /* ===================== BLOCK PREP ===================== */
      const transactionCount = blockTxData?.txs?.length ?? 0;

      const blockDetails = {
        miner,
        validatorOperatorAddress,
        blocknumber: height,
        timestamp: time ?? "",
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

      const sessionData = await ExlorerHelper.getNodeEarning(
        blockDetails.blocknumber,
      );

      if (sessionData) {
        const fullSessionData = {
          ...sessionData,
          blocknumber: message.result.data?.value?.block?.header?.height,
          startAt: message.result.data?.value?.block?.header?.time || null,
        };

        await dbHelper.saveSessionTransactions(fullSessionData);
      }

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
        blockDataToSave,
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
          blockDataToSave.miner,
        ),
        RedisService.setString(REDIS_KEY.LATEST_BLOCK, height.toString()),
      ]);

      return true;
    } catch (error) {
      console.error(
        "Error processing block:",
        message?.result?.data?.value?.block?.header?.height,
        error,
      );
      return false;
    }
  }

  async processDelegatorRewards(
    address: string,
    rewardEvent: string[],
    timeStamp: string,
  ) {
    let rewardAmount = BigNumber(0);

    for (let i = 0; i < rewardEvent?.length; i++) {
      const element = rewardEvent[i];
      let amountArr: any[] = [];

      if (
        typeof element === "string" &&
        element.includes(environment.symbol.toLowerCase())
      ) {
        amountArr = element.split("t");
      }
      rewardAmount = rewardAmount.plus(amountArr[0] || 0);
    }

    console.log("rewardAmount:  ", rewardAmount.toString());

    const rewardData = await prisma.rewards.findFirst({
      where: {
        address,
      },
    });

    if (!rewardData) {
      console.log(
        "Reward not Found in DataBase, so saving....... in db and redis",
        {
          address,
          timeStamp,
          rewardAmount: rewardAmount.toString(),
        },
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
        "Reward Found in DataBase..... and time not Matched ",
        new Date(timeStamp).getTime(),
        new Date(rewardData?.timeStamp).getTime(),
        timeCondition,
      );

      if (timeCondition) {
        const newRewardToSet = rewardAmount
          .plus(rewardData.rewardAmount)
          .toString();

        console.log("Updating new Reward in table and redis ", newRewardToSet);

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
   * process latest transactions (from v2stable - comprehensive version with all transaction types)
   * @param message
   * @returns
   */
  public async processTxs(message: any) {
    try {
      const events = message?.result?.events;
      const data = message?.result?.data;
      if (data && events) {
        const code = data?.value?.TxResult?.result?.code;

        const status = !code
          ? TRANSACTION_STATUS.SUCCESS
          : TRANSACTION_STATUS.FAILED;

        let txEvent;
        txEvent = events?.["message.action"]
          ? events?.["message.action"][0]
          : "";

        const logs: string = data?.value?.TxResult?.result?.log;
        const hash = events["tx.hash"] ? `${events["tx.hash"][0]}` : "";
        console.log("--TXhash--: ", hash);

        let type: string;
        let val: string | number = 0;
        let contractAddress = "";
        let subscriptionId = "";
        let planId: any = "";
        let sessionId = "";
        let dvpnStatus = "";
        let accAddress = "";
        let provAddress = "";
        let nodeAddress = "";

        let stackingRewardValue: string | number = 0;
        let inactiveAt = null;
        const firstIndexAddress: string = events["message.sender"]
          ? `${events["message.sender"][0]}`
          : "";
        const actionSender = getActionAndSender(events);
        let fromAddress = actionSender?.sender ?? firstIndexAddress;

        let toAddress = "";
        let method = "";
        let timeStamp = "";

        const blocknumber = Number(
          message.result.data?.value?.TxResult?.height,
        );

        const url = `${environment.httpHost}/block?height=${blocknumber}`;

        const fetchRes = await fetchRequest(
          url,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null,
        );
        console.log(fetchRes, "Tx-Message-swagger");

        if (!fetchRes.error) {
          console.log(
            "timestamp  : ",
            fetchRes?.data.result.block?.header?.time,
          );
          timeStamp = fetchRes?.data.result.block?.header?.time ?? "";
        }

        switch (txEvent) {
          // ---------------------------------------------
          // BANK TRANSFER
          // ---------------------------------------------
          case "/cosmos.bank.v1beta1.MsgSend":
            type = TRANSACTION_TYPE.COIN_TRANSFER;
            val =
              events["transfer.amount"]?.[1] ??
              events["transfer.amount"]?.[0] ??
              0;
            toAddress =
              events["transfer.recipient"]?.[1] ??
              events["transfer.recipient"]?.[0] ??
              "";
            break;

          // ---------------------------------------------
          // DELEGATION
          // ---------------------------------------------
          case "/cosmos.staking.v1beta1.MsgDelegate":
            if (events["message.sender"]?.length === 1) {
              type = TRANSACTION_TYPE.CREATE_DELEGATOR;
            } else {
              type = TRANSACTION_TYPE.BOND_MORE;
            }

            val = events["delegate.amount"]?.[0] ?? 0;
            toAddress = "/cosmos.staking.v1beta1.MsgDelegate";

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );

            break;

          // ---------------------------------------------
          // UNDELEGATION
          // ---------------------------------------------
          case "/cosmos.staking.v1beta1.MsgUndelegate":
            type = TRANSACTION_TYPE.UNBOND_FUNDS;
            val = events["unbond.amount"]?.[0] ?? 0;
            toAddress = "/cosmos.staking.v1beta1.MsgUndelegate";

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          // ---------------------------------------------
          // CREATE VALIDATOR
          // ---------------------------------------------
          case "/cosmos.staking.v1beta1.MsgCreateValidator":
            type = TRANSACTION_TYPE.CREATE_VALIDATOR;
            val = events["create_validator.amount"]?.[0] ?? 0;
            toAddress = "/cosmos.staking.v1beta1.MsgCreateValidator";

            break;

          case "/cosmos.gov.v1beta1.MsgSubmitProposal":
            type = TRANSACTION_TYPE.SUBMIT_PROPOSAL;
            val = events["transfer.amount"] ? events["transfer.amount"][0] : 0;
            toAddress = "/cosmos.gov.v1.MsgSubmitProposal";

            break;

          case "/cosmos.gov.v1beta1.MsgVote":
            type = TRANSACTION_TYPE.VOTING;
            val = events["transfer.amount"] ? events["transfer.amount"][0] : 0;
            toAddress = "/cosmos.gov.v1.MsgVote";

            break;

          // Start Subscription
          case "/qubetics.subscription.v3.MsgStartSubscriptionRequest":
            type = TRANSACTION_TYPE.START_SUBSCRIPTION;
            val = events["qubetics.subscription.v3.EventPay.payment"]?.[0] ?? 0;
            stackingRewardValue =
              events["qubetics.subscription.v3.EventPay.staking_reward"]?.[0] ??
              0;
            provAddress =
              events[
                "qubetics.subscription.v3.EventCreate.prov_address"
              ]?.[0] ?? "";
            accAddress =
              events["qubetics.subscription.v3.EventCreate.acc_address"]?.[0] ??
              "";
            subscriptionId =
              events["qubetics.subscription.v3.EventCreate.id"]?.[0] ?? "";
            planId =
              events["qubetics.subscription.v3.EventCreate.plan_id"]?.[0] ?? "";

            break;

          // Start Session
          case "/qubetics.subscription.v3.MsgStartSessionRequest":
            type = TRANSACTION_TYPE.START_REQUEST;

            nodeAddress =
              events[
                "qubetics.subscription.v3.EventCreateSession.node_address"
              ]?.[0] ?? "";
            accAddress =
              events[
                "qubetics.subscription.v3.EventCreateSession.acc_address"
              ]?.[0] ?? "";
            subscriptionId =
              events[
                "qubetics.subscription.v3.EventCreateSession.subscription_id"
              ]?.[0] ?? "";
            sessionId =
              events["qubetics.subscription.v3.EventCreateSession.id"]?.[0] ??
              "";

            break;

          // End Session
          case "/qubetics.session.v3.MsgCancelSessionRequest":
            type = TRANSACTION_TYPE.END_REQUEST;
            provAddress =
              events[
                "qubetics.session.v3.EventUpdateStatus.prov_address"
              ]?.[0] ?? "";
            nodeAddress =
              events[
                "qubetics.session.v3.EventUpdateStatus.node_address"
              ]?.[0] ?? "";
            sessionId =
              events["qubetics.session.v3.EventUpdateStatus.id"]?.[0] ?? "";
            dvpnStatus =
              events["qubetics.session.v3.EventUpdateStatus.status"]?.[0] ?? "";

            break;

          // End Subscription
          case "/qubetics.subscription.v3.MsgCancelSubscriptionRequest":
            type = TRANSACTION_TYPE.END_SUBSCRIPTION;

            accAddress =
              events["qubetics.subscription.v3.EventUpdate.acc_address"]?.[0] ??
              "";
            subscriptionId =
              events["qubetics.subscription.v3.EventUpdate.id"]?.[0] ?? "";
            planId =
              events["qubetics.subscription.v3.EventUpdate.plan_id"]?.[0] ?? "";
            dvpnStatus =
              events["qubetics.subscription.v3.EventUpdate.status"]?.[0] ?? "";

            break;

          case "/cosmos.authz.v1beta1.MsgExec":
            type = TRANSACTION_TYPE.RESTAKING_EXECUTION;
            val = events["transfer.amount"] ? events["transfer.amount"][0] : 0;
            toAddress = "/cosmos.authz.v1beta1.MsgExec";

            break;

          case "/cosmos.staking.v1beta1.MsgEditValidator":
            type = TRANSACTION_TYPE.EDIT_VALIDATOR;
            toAddress = "/cosmos.staking.v1beta1.MsgEditValidator";
            break;

          // ---------------------------------------------
          // REDELEGATION
          // ---------------------------------------------
          case "/cosmos.staking.v1beta1.MsgBeginRedelegate":
            type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
            toAddress = "/cosmos.staking.v1beta1.MsgBeginRedelegate";
            val = events["redelegate.amount"]?.[0] ?? 0;

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          // ---------------------------------------------
          // CANCEL UNBONDING
          // ---------------------------------------------
          case "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation":
            type = TRANSACTION_TYPE.CANCEL_UNBONDING_DELEGATION;
            val = events["cancel_unbonding_delegation.amount"]?.[0] ?? 0;
            toAddress = "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation";
            break;

          // ---------------------------------------------
          // UNJAIL
          // ---------------------------------------------
          case "/cosmos.slashing.v1beta1.MsgUnjail":
            type = TRANSACTION_TYPE.UNJAIL;
            toAddress = "/cosmos.slashing.v1beta1.MsgUnjail";
            break;

          // ---------------------------------------------
          // WITHDRAW REWARDS (Delegator)
          // ---------------------------------------------
          case "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward":
            type = TRANSACTION_TYPE.DELEGATOR_REWARD;
            toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward";
            val = events["transfer.amount"]?.[0] ?? 0;

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          // ---------------------------------------------
          // WITHDRAW COMMISSION (Validator)
          // ---------------------------------------------
          case "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission":
            type = TRANSACTION_TYPE.VALIDATOR_COMMISSION;
            toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission";
            val = events["withdraw_rewards.amount"]?.[0] ?? 0;
            break;

          // ---------------------------------------------
          // COSMWASM CONTRACTS
          // ---------------------------------------------

          case "/cosmwasm.wasm.v1.MsgInstantiateContract":
            type = TRANSACTION_TYPE.CONTRACT_CREATION;
            contractAddress =
              events["instantiate._contract_address"]?.[0] ?? "";
            break;

          case "/cosmwasm.wasm.v1.MsgExecuteContract":
            type = TRANSACTION_TYPE.CONTRACT_EXECUTE;
            contractAddress = events["execute._contract_address"]?.[0] ?? "";
            val = events["wasm.amount"]?.[0] ?? 0;
            method = events["wasm.action"]?.[0] ?? "";
            break;

          // ---------------------------------------------
          // IBC TRANSFERS
          // ---------------------------------------------
          case "/ibc.applications.transfer.v1.MsgTransfer":
            type = TRANSACTION_TYPE.IBC_TRANSFER_INITIATE;
            val = events["transfer.amount"]?.[0] ?? 0;
            toAddress = events["ibc_transfer.receiver"]?.[0] ?? "";
            break;

          case "/ibc.core.client.v1.MsgUpdateClient":
            txEvent = events?.["message.action"]?.[1] ?? "";

            switch (txEvent) {
              case "/ibc.core.channel.v1.MsgRecvPacket":
                type = TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE;
                val = events["coin_spent.amount"]?.[0] ?? 0;
                toAddress = events["fungible_token_packet.receiver"]?.[0] ?? "";
                fromAddress = events["fungible_token_packet.sender"]?.[0] ?? "";
                break;

              case "/ibc.core.channel.v1.MsgAcknowledgement":
                type = TRANSACTION_TYPE.IBC_TRANSFER_CONFIRM;
                val = events["fungible_token_packet.amount"]?.[0] ?? 0;
                toAddress = events["fungible_token_packet.receiver"]?.[0] ?? "";
                fromAddress = events["fungible_token_packet.sender"]?.[0] ?? "";
                break;

              default:
                type = "UNKNOWN";
                break;
            }
            break;

          // ---------------------------------------------
          // EVM TX (Ethermint)
          // ---------------------------------------------
          case "/ethermint.evm.v1.MsgEthereumTx":
            type = TRANSACTION_TYPE.COIN_RECEIVE;
            val = events["ethereum_tx.amount"]?.[0] ?? 0;
            toAddress = events["transfer.recipient"]?.[1] ?? "";
            break;

          // ---------------------------------------------
          // DEFAULT / UNKNOWN
          // ---------------------------------------------
          default:
            type = txEvent;
            break;
        }

        let amount;
        let stackingReward;

        if (val) {
          if (type === TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE) {
            amount = noExponent(Number(retriveNumbers(val)) / 10 ** 6);
          } else if (type === TRANSACTION_TYPE.START_SUBSCRIPTION) {
            amount = parseTics(val);
            stackingReward = parseTics(stackingRewardValue);
          } else {
            amount = noExponent(Number(retriveNumbers(val)) / 10 ** 18);
          }
        } else {
          amount = val;
        }
        console.log("amount---", amount);

        // const txFeeData = events?['tx.fee']?[0]
        //   ? events['tx.fee'][0].match(/[\d.]+/g)[0]
        //   : '';

        const txFeeData = events?.["tx.fee"]?.[0]?.match(/[\d.]+/g)?.[0] ?? 0;

        if (planId) {
          planId = Number(planId.replace(/"/g, ""));

          try {
            const planDetailsUrl = `${environment.evmHost}/qubetics/plan/v3/plans/${planId}`;

            const planResponse = await fetchRequest(
              planDetailsUrl,
              FETCH_METHODS.GET,
              HEADERS.DEFAULT,
              null,
            );

            if (!planResponse.error) {
              inactiveAt = addHours(timeStamp, planResponse?.data?.plan?.hours);
            }
          } catch (err) {
            console.log(err, "error");
          }
        }

        const txFee = txFeeData
          ? (Number(txFeeData) / 10 ** 18).toString()
          : txFeeData.toString();

        const txData = {
          type: getType(type),
          status,
          value: amount ? amount?.toString() : "0",
          timestamp: timeStamp,
          txString: message.result.data?.value?.TxResult?.tx ?? "",
          txhash: `0x${hash ?? ""}`,
          toAddress: cleanEscapedString(toAddress),
          blocknumber: Number(message.result.data?.value?.TxResult?.height),
          fromAddress: cleanEscapedString(fromAddress),
          nodeAddress: cleanEscapedString(nodeAddress),
          txFee: String(txFee),
          gasWanted: Number(
            message.result.data?.value?.TxResult?.result?.gas_wanted ?? "",
          ),
          gasUsed: Number(
            message.result.data?.value?.TxResult?.result?.gas_used ?? "",
          ),
          contractAddress,
        };

        socketEventsHelper.emitMessage(
          SOCKET_EVENT.NATIVE_LATEST_TRANSACTIONS,
          txData,
        );
        console.log("txData : ", txData);

        // save transation
        await dbHelper.saveTransactions(txData);

        if (txData.type === TRANSACTION_TYPE.VOTING) {
          let answer = events["proposal_vote.option"]
            ? events["proposal_vote.option"][0].split(" ")[0].split(":")[1]
            : "";

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
            proposalId: events["proposal_vote.proposal_id"]
              ? events["proposal_vote.proposal_id"][0]
              : "",
            answer,
            txHash: txData.txhash,
            blockNumber: events["tx.height"] ? events["tx.height"][0] : "",
            voter: events["message.sender"]
              ? `${events["message.sender"][0]}`
              : "",
          };

          // save the details of voters
          const voterSaveRes = await dbHelper.saveVoters(voters);

          console.log("voterSaveRes : ", voterSaveRes);
        }

        let contractType = "UNKNOWN";
        let burnedFee;
        if (
          txData.type === TRANSACTION_TYPE.START_SUBSCRIPTION ||
          txData.type === TRANSACTION_TYPE.START_REQUEST ||
          txData.type === TRANSACTION_TYPE.END_REQUEST ||
          txData.type === TRANSACTION_TYPE.END_SUBSCRIPTION
        ) {
          const sessionData = {
            logs: logs || "",
            txhash: `0x${hash ?? ""}`,
            subscriptionId,
            planId: planId.toString() ?? "",
            sessionId,
            status: dvpnStatus ?? "",
            type: txEvent,
            provAddress: cleanEscapedString(provAddress),
            accAddress: cleanEscapedString(accAddress),
            nodeAddress: cleanEscapedString(nodeAddress),
            price: amount ? amount?.toString() : "",
            stackingReward: stackingReward ? stackingReward?.toString() : "",
            blocknumber: txData.blocknumber,
            startAt: txData.timestamp || null,
            inactiveAt: inactiveAt || null,
          };
          console.log("Calling in sessionData!!!");

          await dbHelper.saveSessionTransactions(sessionData);
        }

        if (txData.type === TRANSACTION_TYPE.CONTRACT_CREATION) {
          // call the method to get the type of contract
          if (txData.contractAddress) {
            contractType = await this.cosmjsInstace.getContractType(
              txData.contractAddress,
            );
            if (contractType === null) {
              contractType = "UNKNOWN";
            }
          }

          const contractDetails: IContracts = {
            contractName: "",
            txHash: txData.txhash,
            blockNumber: events["tx.height"] ? events["tx.height"][0] : "",
            address: events["instantiate._contract_address"]
              ? events["instantiate._contract_address"][0]
              : "",
            contractType,
            timestamp: txData.timestamp,
            creator: events["message.sender"]
              ? events["message.sender"][0]
              : "",
          };

          const contractName = await this.cosmjsInstace.getContractName(
            contractDetails.address,
          );

          if (contractName !== null) {
            contractDetails.contractName = contractName;
          }
          // save the details of contract

          await dbHelper.saveContractDeployTx(contractDetails);

          const tokenInfo = await this.cosmjsInstace.getTokenInfo(
            contractDetails.address,
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
              contractAddress: contractDetails?.address,
            };

            // save the details of token
            await dbHelper.saveToken(tokens);
          }
        } else if (txData.type === TRANSACTION_TYPE.CONTRACT_EXECUTE) {
          burnedFee = (Number(txData?.txFee) * 0.2).toFixed(11);

          const contractLogData: IContractLogs = {
            logs,
            method,
            txFee: txData.txFee,
            txHash: txData.txhash,
            status: txData.status,
            timestamp: txData.timestamp,
            contractAddress: txData.contractAddress,
            blockNumber: Number(txData.blocknumber),
            burnedFee: Number(burnedFee),
          };

          await dbHelper.saveContractLogs(contractLogData);

          await dbHelper.updateContractExec(txData.contractAddress);

          const account = await this.cosmjsInstace.getAccountInfo(
            txData.contractAddress,
          );

          await this.cosmjsInstace.getTokenBalances(
            txData.contractAddress,
            account,
          );
        }

        const getBurnedAmount = await RedisService.getString(
          REDIS_KEY.TOTAL_BURNED_AMOUNT,
        );

        if (!getBurnedAmount) {
          const burnedAmount = await prisma.transactions.aggregate({
            _sum: {
              burnedFee: true,
            },
          });

          const setBurnedAmount =
            Number(burnedAmount?._sum.burnedFee) +
              Number(STATIC_BURNED_AMOUNT) || Number(STATIC_BURNED_AMOUNT);

          await RedisService.setString(
            REDIS_KEY.TOTAL_BURNED_AMOUNT,
            setBurnedAmount,
          );
        } else {
          await redisHelper.incrementValue(
            REDIS_KEY.TOTAL_BURNED_AMOUNT,
            Number(burnedFee),
          );
        }
      }
      return true;
    } catch (error) {
      console.log("Error while process latest transactions: ", error);
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
        null,
      );
      if (fetchRes?.data?.result && !fetchRes.error) {
        console.log(
          "tx Data inside missedTx : ",
          JSON.stringify(fetchRes?.data),
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
          txString: fetchRes.data?.result?.tx ?? "",
          blocknumber: Number(fetchRes.data?.result?.height ?? 0),
          txFee: fetchRes.data.result?.tx_result?.gas_used ?? "",
          gasWanted: fetchRes.data.result?.tx_result?.gas_wanted ?? "",
          gasUsed: fetchRes.data.result?.tx_result?.gas_used ?? "",
          type: "",
          toAddress: "",
          fromAddress: "",
          value: "",
          timestamp: "",
          contractAddress: "",
          burnedFee: Number(burnedFee),
        };

        const filteredEvents = events.filter(
          (event: any) =>
            event.type === "message" &&
            event?.attributes.some((attr: any) => attr.key === "action"),
        );

        const eventObj = filteredEvents[0]?.attributes.find(
          (i: any) => i.key === "action",
        );
        const txEvent = eventObj?.value || "";

        switch (txEvent) {
          case "/cosmos.bank.v1beta1.MsgSend":
            txData.type = TRANSACTION_TYPE.COIN_TRANSFER;
            break;
          case "/cosmos.staking.v1beta1.MsgDelegate":
            txData.type = TRANSACTION_TYPE.BOND_MORE;
            break;

          case "/cosmos.staking.v1beta1.MsgUndelegate":
            txData.type = TRANSACTION_TYPE.UNBOND_FUNDS;
            break;

          case "/cosmwasm.wasm.v1.MsgInstantiateContract":
            txData.type = TRANSACTION_TYPE.CONTRACT_CREATION;
            break;

          case "/cosmwasm.wasm.v1.MsgExecuteContract":
            txData.type = TRANSACTION_TYPE.CONTRACT_EXECUTE;
            break;

          default:
            break;
        }

        const transferEvent = events.find((i: any) => i.type === "transfer");

        transferEvent?.attributes?.forEach((i: any) => {
          switch (i.key) {
            case "recipient":
              txData.toAddress = i?.value || "";
              break;

            case "sender":
              txData.fromAddress = i?.value || "";
              break;

            case "amount": {
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
          null,
        );
        if (!fetchRes2.error) {
          txData.timestamp = fetchRes2?.data.result.block?.header?.time ?? "";
        }

        console.log("start missed tx+++++++++++++", Date.now());
        const saveRes = await dbHelper.saveTransactions(txData);
        console.log("end missed tx+++++++++++++", Date.now());
        console.log("txData: ", saveRes, txData);
        return !saveRes.error;
      }
      throw new Error("Error while getting TX by hash ");
    } catch (error) {
      console.log("error while processing missed txs : ", error);
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
      console.log("block Number", blockNumber);
      const url2 = `${environment.httpHost}/block?height=${blockNumber}`;
      const fetchRes2 = await fetchRequest(
        url2,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null,
      );

      const sessionData = await ExlorerHelper.getNodeEarning(blockNumber);

      if (sessionData) {
        const fullSessionData = {
          ...sessionData,
          blocknumber: fetchRes2?.data.result.block?.header?.height,
          startAt: fetchRes2?.data.result.block?.header?.time || null,
        };

        await dbHelper.saveSessionTransactions(fullSessionData);
      }

      if (!fetchRes2?.data?.result) {
        console.log("fetchRes2?.data ", fetchRes2);
      }

      const validatorOperatorAddress: string =
        <string>(
          await this.cosmjsInstace.tendermintValAddressToValoperAddress(
            fetchRes2?.data.result.block?.header?.proposer_address,
          )
        ) ?? "";

      const miner =
        <string>(
          await this.cosmjsInstace.getDelegatorAddress(validatorOperatorAddress)
        ) ?? "";

      if (!fetchRes2.error) {
        const dataToSave: IBlock = {
          miner,
          validatorOperatorAddress,
          blocknumber: Number(blockNumber),
          blockhash: `0x${fetchRes2?.data?.result?.block_id?.hash ?? ""}`,
          timestamp: fetchRes2?.data.result.block?.header?.time ?? "",
          transactionCount:
            fetchRes2?.data.result.block?.data?.txs?.length ?? 0,
        };

        // console.log('dataToSave: block :  ', dataToSave);

        dbHelper.saveBlockData(dataToSave);
      } else {
        throw new Error("Error while getting block by number ");
      }
      return true;
    } catch (error) {
      console.log("error while processing error blocks : ", error);
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
      throw new Error("Error while saving missed blocks");
    } catch (error) {
      console.log("error while processing missed blocks : ", error);
      return false;
    }
  }

  /**
   * calculate downtime blocks (merged version with improved logging)
   */
  public async downTimeBlocks() {
    try {
      const lastBlockSaved = await RedisService.getString(
        REDIS_KEY.LATEST_BLOCK,
      );
      const blockRes = await fetchRequest(
        `${environment.httpHost}/block`,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null,
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
              null,
            );
            const currentBlock =
              currentBlockRes?.data?.result?.block?.header?.height;

            if (i <= currentBlock) {
              RabbitMqService.inQueueData(
                QUEUE_NAME.DOWNTIME_BLOCK,
                i.toString(),
              );
            }
            // saving missed block to downtime block Queue
          }
          console.log(`Enqueued ${diff - 1} downtime blocks.`);
        } else {
          console.log("socket service is upto date.");
        }
      }

      // Update Redis after processing
      // await RedisService.setString(
      //   REDIS_KEY.LATEST_BLOCK,
      //   current.toString()
      // );
    } catch (error) {
      console.error("Error while getting downtime blocks:", error);
    }
  }

  public async processNullTimestamp(blockData: IBlock) {
    try {
      if (blockData?.blocknumber && blockData?.blocknumber) {
        const saveRes = await dbHelper.saveBlockData(blockData);
        return !saveRes.error;
      }
      throw new Error("Error while saving missed blocks");
    } catch (error) {
      console.log("error while processing missed blocks : ", error);
      return false;
    }
  }

  /**
   * Alternative optimized block processor with analytics (from v2stable)
   * @param message
   * @returns
   */
  public async processLatestBlock_v(message: any): Promise<boolean> {
    try {
      // Early validation
      const blockData = message?.result?.data?.value?.block;
      if (!blockData?.header) {
        console.warn("Invalid block data received");
        return false;
      }

      const { header, data: blockTxData } = blockData;
      const { height, time, proposer_address } = header;

      log("Processing blockNumber:", COLORS.cyan, height, COLORS.yellow);

      // Parallel fetch of validator and miner addresses
      const validatorOperatorAddress =
        await this.cosmjsInstace.tendermintValAddressToValoperAddress(
          proposer_address,
        );

      const miner = await this.cosmjsInstace.getDelegatorAddress(
        validatorOperatorAddress,
      );

      // Prepare block details
      const transactionCount = blockTxData?.txs?.length ?? 0;
      const blockDetails = {
        miner,
        validatorOperatorAddress,
        blocknumber: height,
        timestamp: time ?? "",
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
            null,
          ),
        ]);

      // Emit transaction summary
      const totalTx = await prisma.transactions.count();
      SocketEventEmitter.emitMessage(SOCKET_EVENT.TX_SUMMARY, {
        tpb: transactionCount,
        totalTx,
      });

      // Emit analytics if successful
      if (analytics.status === "fulfilled") {
        SocketEventEmitter.emitMessage(SOCKET_EVENT.ANALYTICS, analytics.value);
      }

      // Save session data if available
      if (sessionData.status === "fulfilled" && sessionData.value) {
        const fullSessionData = {
          ...sessionData.value,
          blocknumber: height,
          startAt: time || null,
        };
        await dbHelper.saveSessionTransactions(fullSessionData);
      }

      // Process block hash and save block data
      if (
        blockHashResponse.status === "fulfilled" &&
        !blockHashResponse.value.error
      ) {
        const blockHash = `0x${blockHashResponse.value.data?.result?.block_id?.hash ?? ""}`;
        const blockDataToSave: IBlock = {
          ...blockDetails,
          blockhash: blockHash,
        };

        // Emit latest block event
        SocketEventEmitter.emitMessage(
          SOCKET_EVENT.NATIVE_LATEST_BLOCK,
          blockDataToSave,
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
            blockDataToSave.miner,
          ),
          RedisService.setString(REDIS_KEY.LATEST_BLOCK, height.toString()),
        ]);
      } else {
        const errorMsg =
          blockHashResponse.status === "rejected"
            ? blockHashResponse.reason
            : blockHashResponse.value?.errorStack;
        console.error("Failed to fetch block hash:", errorMsg);
        return false;
      }

      return true;
    } catch (error) {
      console.error(
        "Error processing block:",
        message?.result?.data?.value?.block?.header?.height,
        error,
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
        REDIS_KEY.NATIVE_BLOCKS_COUNT,
      );

      if (!blockCount) {
        // Initialize from database
        const dbCount = await prisma.blocks.count();
        await RedisService.setString(
          REDIS_KEY.NATIVE_BLOCKS_COUNT,
          dbCount.toString(),
        );
      } else {
        // Increment existing count
        await RedisService.setString(
          REDIS_KEY.NATIVE_BLOCKS_COUNT,
          (Number(blockCount) + 1).toString(),
        );
      }
    } catch (error) {
      console.error("Failed to update block count in Redis:", error);
      // Non-critical error - don't throw
    }
  }
}
export default new ProcessHelper();
