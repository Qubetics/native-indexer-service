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
  parseTx,
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
import rabbitmqService from "../service/rabbitmq.service";

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
   * process latest block
   * @param message
   * @returns
   */
  public async processLatestBlock(message: any) {
    try {
      if (message?.result?.data) {
        const validatorOperatorAddress: string =
          await this.cosmjsInstace.tendermintValAddressToValoperAddress(
            message.result.data?.value?.block?.header?.proposer_address,
          );

        const miner = validatorOperatorAddress
          ? await this.cosmjsInstace.getDelegatorAddress(
              validatorOperatorAddress,
            )
          : "";

        const blockDetails = {
          miner,
          validatorOperatorAddress,
          blocknumber: message.result.data?.value?.block?.header?.height,
          timestamp: message.result.data?.value?.block?.header?.time ?? "",
          transactionCount:
            message.result.data?.value?.block?.data?.txs?.length ?? 0,
        };
        log(
          "blockNumber : ",
          COLORS.cyan,
          blockDetails.blocknumber,
          COLORS.yellow,
        );
        /* emit the latest block data */
        // socketEventsHelper.emitMessage(
        // 	SOCKET_EVENT.NATIVE_LATEST_BLOCK,
        // 	blockDetails,
        // );
        const analytics = await dbHelper.getAnalytics();

        /* emit the transactions per block count */
        SocketEventEmitter.emitMessage(SOCKET_EVENT.TX_SUMMARY, {
          tpb: blockDetails?.transactionCount,
          totalTx: await prisma.transactions.count(),
        });

        /* emit  analytics */
        SocketEventEmitter.emitMessage(SOCKET_EVENT.ANALYTICS, analytics);

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

        const url = `${environment.httpHost}/block?height=${blockDetails.blocknumber}`;
        const fetchRes = await fetchRequest(
          url,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null,
        );
        if (!fetchRes.error) {
          const blockDataToSave: IBlock = {
            ...blockDetails,
            blockhash: `0x${fetchRes?.data.result?.block_id?.hash ?? ""}`,
          };

          socketEventsHelper.emitMessage(
            SOCKET_EVENT.NATIVE_LATEST_BLOCK,
            blockDataToSave,
          );

          const saveRes = await dbHelper.saveBlockData(blockDataToSave);

          if (saveRes) {
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
              console.log("blockCount", blockCount);
            }
          }

          await dbHelper.upsertValidatorsCount(
            blockDataToSave.validatorOperatorAddress,
            blockDataToSave.miner,
          );

          if (saveRes.error) throw new Error("Error while save block data");

          await RedisService.setString(
            REDIS_KEY.LATEST_BLOCK,
            Number(blockDetails.blocknumber),
          );
        } else {
          console.log(
            "\x1b[32mSomething went wrong:\n\x1b[0m",
            fetchRes?.errorStack,
          );
        }
      }
      return true;
    } catch (error) {
      console.log("Error while process latest block: ", error);
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
   * process latest transactions
   * @param message
   * @returns
   */
  public async processTxs(message: any) {
    try {
      const events = message?.result?.events;
      let burnedFee;
      console.log("events : ", events);
      const data = message?.result?.data;
      if (data && events) {
        const code = data?.value?.TxResult?.result?.code;

        const status = !code
          ? TRANSACTION_STATUS.SUCCESS
          : TRANSACTION_STATUS.FAILED;

        let txEvent;
        txEvent = events?.["message.action"]
          ? message?.result?.events?.["message.action"][0]
          : "";
        const logs: string = data?.value?.TxResult?.result?.log;
        const hash = events["tx.hash"] ? `${events["tx.hash"][0]}` : "";
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
        // let fromAddress: string = events["message.sender"]
        //   ? `${events["message.sender"][0]}`
        //   : "";
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
        if (!fetchRes.error) {
          console.log(
            "timestamp fromRPC : ",
            fetchRes?.data.result.block?.header?.time,
          );
          timeStamp = fetchRes?.data.result.block?.header?.time ?? "";
        }

        console.log("txEvent : ", txEvent);

        switch (txEvent) {
          case "/cosmos.bank.v1beta1.MsgSend":
            type = TRANSACTION_TYPE.COIN_TRANSFER;
            val = events["transfer.amount"][1] ?? events["transfer.amount"][0];
            toAddress =
              events["transfer.recipient"][1] ??
              events["transfer.recipient"][0];

            break;

          case "/cosmos.staking.v1beta1.MsgDelegate":
            if (
              events["message.sender"] &&
              events["message.sender"].length === 1
            ) {
              type = TRANSACTION_TYPE.CREATE_DELEGATOR;
            } else {
              type = TRANSACTION_TYPE.BOND_MORE;
            }

            val = events["delegate.amount"] ? events["delegate.amount"][0] : 0;
            toAddress = "/cosmos.staking.v1beta1.MsgDelegate";

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          case "/cosmos.staking.v1beta1.MsgUndelegate":
            type = TRANSACTION_TYPE.UNBOND_FUNDS;
            val = events["unbond.amount"] ? events["unbond.amount"][0] : 0;
            toAddress = "/cosmos.staking.v1beta1.MsgUndelegate";

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          case "/cosmos.staking.v1beta1.MsgCreateValidator":
            type = TRANSACTION_TYPE.CREATE_VALIDATOR;
            val = events["create_validator.amount"]
              ? events["create_validator.amount"][0]
              : 0;
            toAddress = "/cosmos.staking.v1beta1.MsgCreateValidator";

            break;

          case "/ibc.applications.transfer.v1.MsgTransfer":
            type = TRANSACTION_TYPE.IBC_TRANSFER_INITIATE;
            val = events["transfer.amount"] ? events["transfer.amount"][0] : 0;
            toAddress = events["ibc_transfer.receiver"]
              ? events["ibc_transfer.receiver"][0]
              : "";

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

          case "/ibc.core.client.v1.MsgUpdateClient":
            txEvent = events?.["message.action"]
              ? message?.result?.events?.["message.action"][1]
              : "";

            switch (txEvent) {
              case "/ibc.core.channel.v1.MsgRecvPacket":
                type = TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE;
                val = events["coin_spent.amount"]
                  ? events["coin_spent.amount"][0]
                  : 0;

                toAddress = events["fungible_token_packet.receiver"]
                  ? events["fungible_token_packet.receiver"][0]
                  : "";
                fromAddress = events["fungible_token_packet.sender"]
                  ? events["fungible_token_packet.sender"][0]
                  : "";

                break;

              case "/ibc.core.channel.v1.MsgAcknowledgement":
                type = TRANSACTION_TYPE.IBC_TRANSFER_CONFIRM;
                val = events["fungible_token_packet.amount"]
                  ? events["fungible_token_packet.amount"][0]
                  : 0;
                toAddress = events["fungible_token_packet.receiver"]
                  ? events["fungible_token_packet.receiver"][0]
                  : "";
                fromAddress = events["fungible_token_packet.sender"]
                  ? events["fungible_token_packet.sender"][0]
                  : "";

                break;

              default:
                type = "UNKNOWN";
                break;
            }

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

          case "/cosmos.staking.v1beta1.MsgBeginRedelegate":
            type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
            toAddress = "/cosmos.staking.v1beta1.MsgBeginRedelegate";
            val = events["redelegate.amount"]
              ? events["redelegate.amount"][0]
              : 0;

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp,
            );
            break;

          case "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation":
            type = TRANSACTION_TYPE.CANCEL_UNBONDING_DELEGATION;
            val = events["cancel_unbonding_delegation.amount"]
              ? events["cancel_unbonding_delegation.amount"][0]
              : 0;
            toAddress = "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation";
            break;

          case "/cosmos.slashing.v1beta1.MsgUnjail":
            type = TRANSACTION_TYPE.UNJAIL;
            toAddress = "/cosmos.slashing.v1beta1.MsgUnjail";
            break;

          case "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward":
            type = TRANSACTION_TYPE.DELEGATOR_REWARD;
            toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward";
            val = events["transfer.amount"] ? events["transfer.amount"][0] : 0;
            break;

          case "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission":
            type = TRANSACTION_TYPE.VALIDATOR_COMMISSION;
            toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission";
            break;

          case "/cosmwasm.wasm.v1.MsgStoreCode":
            type = TRANSACTION_TYPE.CONTRACT_UPLOAD;
            toAddress = "/cosmwasm.wasm.v1.MsgStoreCode";
            break;

          case "/cosmwasm.wasm.v1.MsgInstantiateContract":
            type = TRANSACTION_TYPE.CONTRACT_CREATION;
            contractAddress = events["instantiate._contract_address"]
              ? events["instantiate._contract_address"][0]
              : "";
            break;

          case "/cosmwasm.wasm.v1.MsgExecuteContract":
            type = TRANSACTION_TYPE.CONTRACT_EXECUTE;
            contractAddress = events["execute._contract_address"]
              ? events["execute._contract_address"][0]
              : "";
            val = events["wasm.amount"] ? events["wasm.amount"][0] : 0;
            method = events["wasm.action"] ? events["wasm.action"][0] : 0;
            break;

          case "/ethermint.evm.v1.MsgEthereumTx":
            type = TRANSACTION_TYPE.COIN_RECEIVE;
            val = events["coin_received.amount"]
              ? events["coin_received.amount"][0]
              : 0;
            toAddress = events["transfer.recipient"]
              ? events["transfer.recipient"][1]
              : "";
            break;

          default:
            type = "UNKNOWN";
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

        console.log("txFeeData", events["tx.fee"]);

        // const txFeeData = events["tx.fee"][0]
        //   ? events["tx.fee"][0].match(/[\d.]+/g)[0]
        //   : "";

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

        console.log("txFeeData", txFeeData);

        const txFee = txFeeData
          ? (Number(txFeeData) / 10 ** 18).toString()
          : txFeeData.toString();
        console.log("txFee", txFee);
        burnedFee = (Number(txFee) * 0.2).toFixed(11);
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
          burnedFee: Number(burnedFee),
        };

        socketEventsHelper.emitMessage(
          SOCKET_EVENT.NATIVE_LATEST_TRANSACTIONS,
          txData,
        );

        // save transation
        await dbHelper.saveTransactions(txData);

        if (
          txData.type === TRANSACTION_TYPE.START_SUBSCRIPTION ||
          txData.type === TRANSACTION_TYPE.START_REQUEST ||
          txData.type === TRANSACTION_TYPE.END_REQUEST ||
          txData.type === TRANSACTION_TYPE.END_SUBSCRIPTION
        ) {
          const parsedLogs = (() => { try { return logs ? JSON.parse(logs) : null; } catch { return null; } })();
          const sessionData = {
            logs: parsedLogs,
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
      console.log("error while processing tranactions ", error);
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
      let accAddress = "";
      let provAddress = "";
      let nodeAddress = "";
      let subscriptionId = "";
      let sessionId = "";
      let planId = "";
      let dvpnStatus = "";
      let stackingRewardValue = "";
      let stackingReward = "";

      // Strip any accidental leading 0x — the URL already prepends 0x
      const rawHash = txHash.replace(/^0x/i, "");
      const url = `${environment.httpHost}/tx?hash=0x${rawHash}`;
      const fetchRes = await fetchRequest(
        url,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null,
      );

      // Guard: RPC returned no result (network error, hash not found, etc.)
      if (!fetchRes?.data?.result) {
        return false;
    }

      const result = fetchRes.data.result;
      const logs: string = result?.tx_result?.log;
      const events: any[] = result?.tx_result?.events ?? [];
      const code = result?.tx_result?.code;
      const status = !code
        ? TRANSACTION_STATUS.SUCCESS
        : TRANSACTION_STATUS.FAILED;
      const burnedFee = (
        Number(result?.tx_result?.gas_used) * 0.2
      ).toFixed(11);

      const txData: ITransaction = {
        status,
        txhash: `0x${rawHash}`,
        txString: result?.tx ?? "",
        blocknumber: Number(result?.height ?? 0),
        txFee: result?.tx_result?.gas_used ?? "",
        gasWanted: result?.tx_result?.gas_wanted ?? "",
        gasUsed: result?.tx_result?.gas_used ?? "",
        type: "",
        toAddress: "",
        fromAddress: "",
        value: "",
        timestamp: "",
        contractAddress: "",
      };

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

      let val = 0;

      // Default values
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
            val = i?.value ?? 0;
            break;
          }

          default:
            break;
        }
      });

      const filteredEvents = events.filter(
        (event: any) =>
          event.type === "message" &&
          event?.attributes.some((attr: any) => attr.key === "action"),
      );

      const eventObj = filteredEvents[0]?.attributes.find(
        (i: any) => i.key === "action",
      );
      // const fromObj = filteredEvents[0]?.attributes.find(
      //   (i: any) => i.key === 'sender'
      // );

      const txEvent = eventObj?.value || "";

      switch (txEvent) {
        case "/cosmos.bank.v1beta1.MsgSend":
          txData.type = TRANSACTION_TYPE.COIN_TRANSFER;
          break;

        case "/cosmos.staking.v1beta1.MsgDelegate": {
          txData.type = TRANSACTION_TYPE.BOND_MORE;
          txData.toAddress = "/cosmos.staking.v1beta1.MsgDelegate";

          const delegateEvent = events.find(
            (e: any) => e.type === "delegate",
          );
          const amountAttr = delegateEvent?.attributes.find(
            (e: any) => e.key === "amount",
          );
          val = (amountAttr?.value || "0").replace(/[^\d.]/g, "");

          const withdrawRewardEvent = events.find((e: any) => e.type === "withdraw_rewards");
          const withdrawAmount = withdrawRewardEvent?.attributes?.find((a: any) => a.key === "amount")?.value;
          if (withdrawAmount) {
            this.processDelegatorRewards(
              txData.fromAddress,
              [withdrawAmount],
              txData.timestamp,
            );
          }
          break;
        }

        case "/cosmos.staking.v1beta1.MsgBeginRedelegate": {
          txData.type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
          txData.toAddress = "/cosmos.staking.v1beta1.MsgBeginRedelegate";

          const filtered = events.filter((e: any) => e.type === "redelegate");

          const valArr = filtered[0]?.attributes.filter(
            (e: any) => e.key === "amount",
          );
          val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

          const filterForReward = events.filter(
            (e: any) => e.type === "withdraw_rewards",
          );
          const valArrReward = filterForReward[0]?.attributes.filter(
            (e: any) => e.key === "amount",
          );

          const rewardValArr = valArrReward.map((item: any) => item.value);

          this.processDelegatorRewards(
            txData.fromAddress,
            rewardValArr,
            txData.timestamp,
          );
          break;
        }

        case "/cosmos.staking.v1beta1.MsgUndelegate": {
          txData.type = TRANSACTION_TYPE.UNBOND_FUNDS;
          txData.toAddress = "/cosmos.staking.v1beta1.MsgUndelegate";

          const unbondEvent = events.find((e: any) => e.type === "unbond");
          const amountAttr = unbondEvent?.attributes.find(
            (e: any) => e.key === "amount",
          );
          val = (amountAttr?.value || "0").replace(/[^\d.]/g, "");
          break;
        }

        case "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward": {
          txData.type = TRANSACTION_TYPE.DELEGATOR_REWARD;
          txData.toAddress =
            "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward";

          const rewardEvent = events.find(
            (e: any) => e.type === "withdraw_rewards",
          );
          const amountAttr = rewardEvent?.attributes.find(
            (e: any) => e.key === "amount",
          );
          val = (amountAttr?.value || "0").replace(/[^\d.]/g, "");

          this.processDelegatorRewards(
            txData.fromAddress,
            [val.toString()],
            txData.timestamp,
          );
          break;
        }

        case "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission": {
          txData.type = TRANSACTION_TYPE.VALIDATOR_COMMISSION;
          txData.toAddress =
            "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission";

          const commissionEvent = events.find(
            (e: any) => e.type === "withdraw_commission",
          );
          const amountAttr = commissionEvent?.attributes.find(
            (e: any) => e.key === "amount",
          );
          val = (amountAttr?.value || "0").replace(/[^\d.]/g, "");
          break;
        }

        case "/cosmos.gov.v1beta1.MsgSubmitProposal":
          txData.type = TRANSACTION_TYPE.SUBMIT_PROPOSAL;
          txData.toAddress = "/cosmos.gov.v1.MsgSubmitProposal";
          break;

        case "/cosmos.gov.v1beta1.MsgVote":
          txData.type = TRANSACTION_TYPE.VOTING;
          txData.toAddress = "/cosmos.gov.v1.MsgVote";
          break;

        case "/cosmos.staking.v1beta1.MsgCreateValidator":
          txData.type = TRANSACTION_TYPE.CREATE_VALIDATOR;
          txData.toAddress = "/cosmos.staking.v1beta1.MsgCreateValidator";
          break;

        case "/cosmos.staking.v1beta1.MsgEditValidator":
          txData.type = TRANSACTION_TYPE.EDIT_VALIDATOR;
          txData.toAddress = "/cosmos.staking.v1beta1.MsgEditValidator";
          break;

        case "/cosmos.slashing.v1beta1.MsgUnjail":
          txData.type = TRANSACTION_TYPE.UNJAIL;
          txData.toAddress = "/cosmos.slashing.v1beta1.MsgUnjail";
          break;

        case "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation": {
          txData.type = TRANSACTION_TYPE.CANCEL_UNBONDING_DELEGATION;
          txData.toAddress =
            "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation";

          const filtered = events.filter(
            (e: any) => e.type === "cancel_unbonding_delegation",
          );
          const valArr = filtered[0]?.attributes.filter(
            (e: any) => e.key === "amount",
          );
          val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
          break;
        }


        case "/qubetics.subscription.v3.MsgStartSubscriptionRequest": {
          txData.type = TRANSACTION_TYPE.START_SUBSCRIPTION;

          const payEvent = events.find((e: any) => e.type === "qubetics.subscription.v3.EventPay");
          const createEvent = events.find((e: any) => e.type === "qubetics.subscription.v3.EventCreate");

          val = payEvent?.attributes?.find((a: any) => a.key === "payment")?.value ?? 0;
          stackingRewardValue = payEvent?.attributes?.find((a: any) => a.key === "staking_reward")?.value ?? "";
          provAddress = createEvent?.attributes?.find((a: any) => a.key === "prov_address")?.value ?? "";
          accAddress = createEvent?.attributes?.find((a: any) => a.key === "acc_address")?.value ?? "";
          subscriptionId = createEvent?.attributes?.find((a: any) => a.key === "id")?.value ?? "";
          planId = createEvent?.attributes?.find((a: any) => a.key === "plan_id")?.value ?? "";

          break;
        }

        case "/qubetics.subscription.v3.MsgStartSessionRequest": {
          txData.type = TRANSACTION_TYPE.START_REQUEST;

          const createSessionEvent = events.find((e: any) => e.type === "qubetics.subscription.v3.EventCreateSession");

          nodeAddress = createSessionEvent?.attributes?.find((a: any) => a.key === "node_address")?.value ?? "";
          accAddress = createSessionEvent?.attributes?.find((a: any) => a.key === "acc_address")?.value ?? "";
          subscriptionId = createSessionEvent?.attributes?.find((a: any) => a.key === "subscription_id")?.value ?? "";
          sessionId = createSessionEvent?.attributes?.find((a: any) => a.key === "id")?.value ?? "";

          break;
        }

        case "/qubetics.session.v3.MsgCancelSessionRequest": {
          txData.type = TRANSACTION_TYPE.END_REQUEST;

          const updateStatusEvent = events.find((e: any) => e.type === "qubetics.session.v3.EventUpdateStatus");

          provAddress = updateStatusEvent?.attributes?.find((a: any) => a.key === "prov_address")?.value ?? "";
          nodeAddress = updateStatusEvent?.attributes?.find((a: any) => a.key === "node_address")?.value ?? "";
          sessionId = updateStatusEvent?.attributes?.find((a: any) => a.key === "id")?.value ?? "";
          dvpnStatus = updateStatusEvent?.attributes?.find((a: any) => a.key === "status")?.value ?? "";

          break;
        }

        case "/qubetics.subscription.v3.MsgCancelSubscriptionRequest": {
          txData.type = TRANSACTION_TYPE.END_SUBSCRIPTION;

          const updateEvent = events.find((e: any) => e.type === "qubetics.subscription.v3.EventUpdate");

          accAddress = updateEvent?.attributes?.find((a: any) => a.key === "acc_address")?.value ?? "";
          subscriptionId = updateEvent?.attributes?.find((a: any) => a.key === "id")?.value ?? "";
          planId = updateEvent?.attributes?.find((a: any) => a.key === "plan_id")?.value ?? "";
          dvpnStatus = updateEvent?.attributes?.find((a: any) => a.key === "status")?.value ?? "";

          break;
        }

        default:
          txData.type = getType(txEvent) || txEvent || "UNKNOWN";
          break;
      }

      let amount;
      let inactiveAt = null;

      if (txData.type === TRANSACTION_TYPE.START_SUBSCRIPTION) {
        amount = parseTics(val);
        stackingReward = parseTics(stackingRewardValue);
      } else if (val) {
        if (txData.type === TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE) {
          amount = noExponent(Number(retriveNumbers(val)) / 10 ** 6);
        } else {
          amount = noExponent(Number(retriveNumbers(val)) / 10 ** 18);
        }
      }
      txData.value = amount ? amount.toString() : "0";

      if (planId) {
        const numericPlanId = Number(String(planId).replace(/"/g, ""));

        try {
          const planDetailsUrl = `${environment.evmHost}/qubetics/plan/v3/plans/${numericPlanId}`;

          const planResponse = await fetchRequest(
            planDetailsUrl,
            FETCH_METHODS.GET,
            HEADERS.DEFAULT,
            null
          );

          if (!planResponse.error) {
            inactiveAt = addHours(txData.timestamp, planResponse?.data?.plan?.hours);
          }

        } catch (err) {
          console.log(err, "error");
        }
      }

      if (
        txData.type === TRANSACTION_TYPE.START_SUBSCRIPTION ||
        txData.type === TRANSACTION_TYPE.START_REQUEST ||
        txData.type === TRANSACTION_TYPE.END_REQUEST ||
        txData.type === TRANSACTION_TYPE.END_SUBSCRIPTION
      ) {
        const parsedLogs = (() => { try { return logs ? JSON.parse(logs) : null; } catch { return null; } })();
        const sessionData = {
          logs: parsedLogs,
          txhash: `0x${txHash ?? ""}`,
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

        await dbHelper.saveSessionTransactions(sessionData);
      }

      const saveRes = await dbHelper.saveTransactions(txData);
      return !saveRes.error;

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
      const fetchRes = await fetchRequest(
        url2,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null,
      );

      if (!fetchRes?.data?.result) {
        console.log("fetchRes2?.data ", fetchRes);
        throw new Error("Failed to fetch block data - null result");
      }

      const sessionData = await ExlorerHelper.getNodeEarning(blockNumber);

      if (sessionData) {
        const fullSessionData = {
          ...sessionData,
          blocknumber: fetchRes?.data.result.block?.header?.height,
          startAt: fetchRes?.data.result.block?.header?.time || null,
        };

        await dbHelper.saveSessionTransactions(fullSessionData);
      }

      const validatorOperatorAddress: string =
        <string>(
          await this.cosmjsInstace.tendermintValAddressToValoperAddress(
            fetchRes?.data.result.block?.header?.proposer_address,
          )
        ) ?? "";

      const miner = validatorOperatorAddress
        ? (<string>(
            await this.cosmjsInstace.getDelegatorAddress(
              validatorOperatorAddress,
            )
          ) ?? "")
        : "";

      if (!fetchRes.error) {
        const dataToSave: IBlock = {
          miner,
          validatorOperatorAddress,
          blocknumber: Number(blockNumber),
          blockhash: `0x${fetchRes?.data?.result?.block_id?.hash ?? ""}`,
          timestamp: fetchRes?.data.result.block?.header?.time ?? "",
          transactionCount:
            fetchRes?.data.result.block?.data?.txs?.length ?? 0,
        };

        for (
          let i = 0;
          i < (fetchRes?.data?.result?.block?.data?.txs?.length ?? 0);
          i++
        ) {
          const element = fetchRes?.data?.result?.block?.data?.txs[i];
          const hash = parseTx(element);
          rabbitmqService.inQueueData(QUEUE_NAME.DOWNTIME_TXS, hash.txHash);
        }

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
      // console.log('dataToSave:  missed block :  ', blockData);

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
   * calculate downtime blocks
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
      console.log("newBlock: ", newBlock);
      console.log("lastBlockSaved: ", lastBlockSaved);

      if (newBlock && lastBlockSaved) {
        const diff = Number(newBlock) - Number(lastBlockSaved);
        console.log("diff : ", diff);
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
        } else console.log("socket service is upto date.");
      }
    } catch (error) {
      console.log("error while getting downtimeblocks : ", error);
    }
  }

  public async processNullTimestamp(blockData: IBlock) {
    try {
      // console.log('dataToSave:  missed block :  ', blockData);

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
}
export default new ProcessHelper();
