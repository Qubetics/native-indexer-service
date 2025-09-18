import { BigNumber } from "bignumber.js";
import { v4 } from "uuid";
import {
  log,
  fetchRequest,
  noExponent,
  retriveNumbers,
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
import rabbitmqService from "../service/rabbitmq.service";
import redisHelper from "../service/redis.helper";
import socketEventsHelper from "../service/socketEvents.helper";
import { CosmJsRpcMethods } from "./cosmsClient.helper";
import dbHelper from "./db.helper";
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
      blockWsQuery
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
      txWsQuery
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
        const validatorOperatorAddress: string = <string>(
          await this.cosmjsInstace.tendermintValAddressToValoperAddress(
            message.result.data?.value?.block?.header?.proposer_address
          )
        );

        const miner = <string>(
          await this.cosmjsInstace.getDelegatorAddress(validatorOperatorAddress)
        );

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
          COLORS.yellow
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

        const url = `${environment.httpHost}/block?height=${blockDetails.blocknumber}`;
        const fetchRes = await fetchRequest(
          url,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null
        );
        if (!fetchRes.error) {
          const blockDataToSave: IBlock = {
            ...blockDetails,
            blockhash: `0x${fetchRes?.data.result?.block_id?.hash ?? ""}`,
          };

          socketEventsHelper.emitMessage(
            SOCKET_EVENT.NATIVE_LATEST_BLOCK,
            blockDataToSave
          );

          const saveRes = await dbHelper.saveBlockData(blockDataToSave);

          if (saveRes) {
            // Retrieve the block count from Redis or the database
            let blockCount = await RedisService.getString(
              REDIS_KEY.NATIVE_BLOCKS_COUNT
            );

            if (!blockCount) {
              // First time, get the count from the database
              blockCount = await prisma.blocks.count();
              await RedisService.setString(
                REDIS_KEY.NATIVE_BLOCKS_COUNT,
                blockCount
              );
            } else {
              // Increment the block count
              blockCount = Number(blockCount) + 1;
              await RedisService.setString(
                REDIS_KEY.NATIVE_BLOCKS_COUNT,
                blockCount
              );
              console.log("blockCount", blockCount);
            }
          }

          await dbHelper.upsertValidatorsCount(
            blockDataToSave.validatorOperatorAddress,
            blockDataToSave.miner
          );

          if (saveRes.error) throw new Error("Error while save block data");

          await RedisService.setString(
            REDIS_KEY.LATEST_BLOCK,
            Number(blockDetails.blocknumber)
          );
        } else {
          console.log(
            "\x1b[32mSomething went wrong:\n\x1b[0m",
            fetchRes?.errorStack
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
    timeStamp: string
  ) {
    try {
      let rewardAmount = BigNumber(0);

      for (let i = 0; i < rewardEvent?.length; i++) {
        const element = rewardEvent[i];
        let amountToAdd = "0";

        if (
          typeof element === "string" &&
          element.includes(environment.symbol.toLowerCase())
        ) {
          amountToAdd = element.replace(/[^\d.]/g, "");
          console.log("amountToAdd:  ", amountToAdd);
        }
        rewardAmount = rewardAmount.plus(amountToAdd || 0);
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
          "Reward Found in DataBase..... and time not Matched ",
          new Date(timeStamp).getTime(),
          new Date(rewardData?.timeStamp).getTime(),
          timeCondition
        );

        if (timeCondition) {
          const newRewardToSet = rewardAmount
            .plus(rewardData.rewardAmount)
            .toString();

          console.log(
            "Updating new Reward in table and redis ",
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
      console.log("Error while calculating rewards : ", error);
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
        let fromAddress: string = events["message.sender"]
          ? `${events["message.sender"][0]}`
          : "";
        let toAddress = "";
        let method = "";
        let timeStamp = "";
        const blocknumber = Number(
          message.result.data?.value?.TxResult?.height
        );

        const url = `${environment.httpHost}/block?height=${blocknumber}`;
        const fetchRes = await fetchRequest(
          url,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null
        );
        if (!fetchRes.error) {
          console.log(
            "timestamp fromRPC : ",
            fetchRes?.data.result.block?.header?.time
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
              timeStamp
            );
            break;

          case "/cosmos.staking.v1beta1.MsgUndelegate":
            type = TRANSACTION_TYPE.UNBOND_FUNDS;
            val = events["unbond.amount"] ? events["unbond.amount"][0] : 0;
            toAddress = "/cosmos.staking.v1beta1.MsgUndelegate";

            this.processDelegatorRewards(
              fromAddress,
              events["withdraw_rewards.amount"],
              timeStamp
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
              timeStamp
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
        // console.log('val==---', val);

        if (val) {
          if (type === TRANSACTION_TYPE.IBC_TRANSFER_RECEIVE) {
            amount = noExponent(Number(retriveNumbers(val)) / 10 ** 6);
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

        console.log("txFeeData", txFeeData);

        const txFee = txFeeData
          ? (Number(txFeeData) / 10 ** 18).toString()
          : txFeeData;
        console.log("txFee", txFee);
        const txData = {
          type,
          status,
          value: amount.toString(),
          timestamp: timeStamp,
          txString: message.result.data?.value?.TxResult?.tx ?? "",
          txhash: `0x${hash}`,
          toAddress,
          blocknumber: Number(message.result.data?.value?.TxResult?.height),
          fromAddress,
          txFee,
          gasWanted: Number(
            message.result.data?.value?.TxResult?.result?.gas_wanted ?? ""
          ),
          gasUsed: Number(
            message.result.data?.value?.TxResult?.result?.gas_used ?? ""
          ),
          contractAddress,
        };

        socketEventsHelper.emitMessage(
          SOCKET_EVENT.NATIVE_LATEST_TRANSACTIONS,
          txData
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

        if (txData.type === TRANSACTION_TYPE.CONTRACT_CREATION) {
          // call the method to get the type of contract
          if (txData.contractAddress) {
            contractType = await this.cosmjsInstace.getContractType(
              txData.contractAddress
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
            contractDetails.address
          );

          if (contractName !== null) {
            contractDetails.contractName = contractName;
          }
          // save the details of contract

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
              contractAddress: contractDetails?.address,
            };

            // save the details of token
            await dbHelper.saveToken(tokens);
          }
        } else if (txData.type === TRANSACTION_TYPE.CONTRACT_EXECUTE) {
          const contractLogData: IContractLogs = {
            logs,
            method,
            txFee: txData.txFee,
            txHash: txData.txhash,
            status: txData.status,
            timestamp: txData.timestamp,
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
      const url = `${environment.httpHost}/tx?hash=0x${txHash}`;
      const fetchRes = await fetchRequest(
        url,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      if (fetchRes?.data?.result && !fetchRes.error) {
        const result = fetchRes?.data?.result;

        const events = result?.tx_result?.events;

        console.log("events inside missed tx: ", JSON.stringify(events));

        const code = result?.tx_result?.code;
        const status = !code
          ? TRANSACTION_STATUS.SUCCESS
          : TRANSACTION_STATUS.FAILED;

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
        };

        const url2 = `${environment.httpHost}/block?height=${txData.blocknumber}`;
        const fetchRes2 = await fetchRequest(
          url2,
          FETCH_METHODS.GET,
          HEADERS.DEFAULT,
          null
        );

        console.log("fetchRes2: ", fetchRes2?.error);

        if (!fetchRes2.error) {
          console.log(
            "timeStamp fro RPC: ",
            fetchRes2?.data.result.block?.header?.time
          );

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
              console.log("\n From value from transafer ::: ", i?.value);
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
            event?.attributes.some((attr: any) => attr.key === "action")
        );

        const eventObj = filteredEvents[0]?.attributes.find(
          (i: any) => i.key === "action"
        );
        // const fromObj = filteredEvents[0]?.attributes.find(
        //   (i: any) => i.key === 'sender'
        // );

        const txEvent = eventObj?.value || "";

        console.log("\n\ntxEvent :::: ", txEvent);

        switch (txEvent) {
          case "/cosmos.bank.v1beta1.MsgSend":
            txData.type = TRANSACTION_TYPE.COIN_TRANSFER;
            break;

          case "/cosmos.staking.v1beta1.MsgDelegate": {
            txData.type = TRANSACTION_TYPE.BOND_MORE;
            txData.toAddress = "/cosmos.staking.v1beta1.MsgDelegate";

            const filtered = events.filter((e: any) => e.type === "delegate");
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

            const filterForReward = events.filter(
              (e: any) => e.type === "withdraw_rewards"
            );

            const valArrReward = filterForReward[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );

            const rewardValArr = valArrReward.map((item: any) => item.value);

            console.log("txData : : ", txData);

            this.processDelegatorRewards(
              txData.fromAddress,
              rewardValArr,
              txData.timestamp
            );
            break;
          }

          case "/cosmos.staking.v1beta1.MsgBeginRedelegate": {
            txData.type = TRANSACTION_TYPE.BEGIN_REDELEGATE;
            txData.toAddress = "/cosmos.staking.v1beta1.MsgBeginRedelegate";

            const filtered = events.filter((e: any) => e.type === "redelegate");

            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

            const filterForReward = events.filter(
              (e: any) => e.type === "withdraw_rewards"
            );
            const valArrReward = filterForReward[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );

            const rewardValArr = valArrReward.map((item: any) => item.value);

            this.processDelegatorRewards(
              txData.fromAddress,
              rewardValArr,
              txData.timestamp
            );
            break;
          }

          case "/cosmos.staking.v1beta1.MsgUndelegate": {
            txData.type = TRANSACTION_TYPE.UNBOND_FUNDS;
            txData.toAddress = "/cosmos.staking.v1beta1.MsgUndelegate";

            const filtered = events.filter((e: any) => e.type === "unbond");
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

            const filterForReward = events.filter(
              (e: any) => e.type === "withdraw_rewards"
            );
            const valArrReward = filterForReward[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );

            const rewardValArr = valArrReward.map((item: any) => item.value);

            this.processDelegatorRewards(
              txData.fromAddress,
              rewardValArr,
              txData.timestamp
            );
            break;
          }
          case "/cosmos.gov.v1beta1.MsgSubmitProposal": {
            txData.type = TRANSACTION_TYPE.SUBMIT_PROPOSAL;
            txData.toAddress = "/cosmos.gov.v1.MsgSubmitProposal";

            const filtered = events.filter((e: any) => e.type === "transfer");
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
            break;
          }

          case "/ethermint.evm.v1.MsgEthereumTx": {
            txData.type = TRANSACTION_TYPE.COIN_RECEIVE;

            const filtered = events.filter(
              (e: any) => e.type === "coin_received"
            );
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            const toArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "receiver"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
            txData.toAddress = toArr[0].value;
            break;
          }

          case "/cosmos.staking.v1beta1.MsgCreateValidator": {
            txData.type = TRANSACTION_TYPE.CREATE_VALIDATOR;
            txData.toAddress = "/cosmos.staking.v1beta1.MsgCreateValidator";

            const filtered = events.filter(
              (e: any) => e.type === "create_validator"
            );
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );

            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

            break;
          }

          case "/cosmos.gov.v1beta1.MsgVote": {
            txData.type = TRANSACTION_TYPE.VOTING;
            txData.toAddress = "/cosmos.gov.v1.MsgVote";

            const filtered = events.filter((e: any) => e.type === "transfer");
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
            break;
          }

          case "/cosmos.staking.v1beta1.MsgEditValidator": {
            txData.type = TRANSACTION_TYPE.EDIT_VALIDATOR;
            txData.toAddress = "/cosmos.staking.v1beta1.MsgEditValidator";
            break;
          }

          case "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation": {
            txData.type = TRANSACTION_TYPE.CANCEL_UNBONDING_DELEGATION;
            txData.toAddress =
              "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation";

            const filtered = events.filter(
              (e: any) => e.type === "cancel_unbonding_delegation"
            );
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
            break;
          }

          case "/cosmos.slashing.v1beta1.MsgUnjail": {
            txData.type = TRANSACTION_TYPE.UNJAIL;
            txData.toAddress = "/cosmos.slashing.v1beta1.MsgUnjail";
            break;
          }

          case "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward": {
            txData.type = TRANSACTION_TYPE.DELEGATOR_REWARD;
            txData.toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward";

            const filtered = events.filter(
              (e: any) => e.type === "cancel_unbonding_delegation"
            );
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");
            break;
          }

          case "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission": {
            txData.type = TRANSACTION_TYPE.VALIDATOR_COMMISSION;
            txData.toAddress =
              "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission";
            break;
          }

          case "/cosmos.authz.v1beta1.MsgExec": {
            txData.type = TRANSACTION_TYPE.RESTAKING_EXECUTION;
            txData.toAddress = "/cosmos.authz.v1beta1.MsgExec";

            const filtered = events.filter((e: any) => e.type === "transfer");
            const valArr = filtered[0]?.attributes.filter(
              (e: any) => e.key === "amount"
            );
            val = (valArr[0].value || "0").replace(/[^\d.]/g, "");

            break;
          }

          default:
            txData.type = "UNKNOWN";
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
        txData.value = amount || "0";

        const saveRes = await dbHelper.saveTransactions(txData);
        console.log("txData: ", saveRes, txData);
        return !saveRes.error;
      }
      throw new Error(
        (fetchRes?.errorStack as string) || "Error while fetching tx by hash"
      );
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
      const url2 = `${environment.httpHost}/block?height=${blockNumber}`;
      const fetchRes = await fetchRequest(
        url2,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      if (!fetchRes.error) {
        const validatorOperatorAddress: string =
          <string>(
            await this.cosmjsInstace.tendermintValAddressToValoperAddress(
              fetchRes?.data.result.block?.header?.proposer_address
            )
          ) ?? "";

        const miner =
          <string>(
            await this.cosmjsInstace.getDelegatorAddress(
              validatorOperatorAddress
            )
          ) ?? "";
        const dataToSave: IBlock = {
          miner,
          validatorOperatorAddress,
          blocknumber: Number(blockNumber),
          blockhash: `0x${fetchRes?.data?.result?.block_id?.hash ?? ""}`,
          timestamp: fetchRes?.data.result.block?.header?.time ?? "",
          transactionCount: fetchRes?.data.result.block?.data?.txs?.length ?? 0,
        };

        for (
          let i = 0;
          i < fetchRes?.data?.result?.block?.data?.txs.length || 0;
          i++
        ) {
          const element = fetchRes?.data?.result?.block?.data?.txs[i];
          const hash = parseTx(element);
          rabbitmqService.inQueueData(QUEUE_NAME.DOWNTIME_TXS, hash.txHash);
        }

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
        REDIS_KEY.LATEST_BLOCK
      );
      const blockRes = await fetchRequest(
        `${environment.httpHost}/block`,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
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
