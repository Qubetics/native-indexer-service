import { RES_MSG, REDIS_KEY, CONST_NAME } from '../constant';
import {
	IBlock,
	IContractLogs,
	IContracts,
	// IContracts,
	IESResponse,
	IHolders,
	IProposalData,
	ITokens,
	ITransaction,
	IValidatorStake,
	IValidatorToSave,
	IVoters,
	ValidatorData,
} from '../interface/index';
import { prisma } from '../lib/db';
import logger from '../lib/logger';
import { RedisService } from '../service/index';
import { fetchRequest } from './common.helper';
import SocketHelper from './socket.helper';

class ExlorerHelper {
	/**
	 * save blocks data into db
	 * @param blockData
	 * @returns
	 */
	saveBlockData = async (blockData: IBlock): Promise<IESResponse> => {
		try {
			await prisma.blocks.create({
				data: blockData,
			});
			return {
				error: false,
			};
		} catch (err) {
			const error = JSON.parse(JSON.stringify(err));

			console.log(
				"error.code === 'P2002' unique check fail : ",
				error.code === 'P2002',
			);

			if (error.code === 'P2002') {
				return {
					error: false,
				};
			}
			console.log('block db error : ', err);
			return {
				error: true,
			};
		}
	};

	/**
	 * save validator data into db
	 * @param blockData
	 * @returns
	 */
	upsertValidatorsCount = async (
		operatorAddress: string,
		validatorAddress: string,
	): Promise<IESResponse> => {
		try {
			const dataTosave = {
				operatorAddress,
				address: validatorAddress,
				count: 1,
			};
			const validatorDetails =
				await prisma.validatedBlocksCount.findUnique({
					where: {
						operatorAddress,
					},
				});

			if (validatorDetails) {
				dataTosave.count = Number(validatorDetails.count) + 1;
				await prisma.validatedBlocksCount.update({
					data: dataTosave,
					where: {
						operatorAddress,
					},
				});
			} else {
				await prisma.validatedBlocksCount.create({
					data: dataTosave,
				});
			}
			return {
				error: false,
			};
		} catch (err) {
			console.log('db error upsertValidatorsCount : ', err);
			return {
				error: true,
			};
		}
	};

	/**
	 * save transactions into db
	 * @param txData
	 * @returns
	 */
	saveTransactions = async (txData: ITransaction): Promise<IESResponse> => {
		try {
			await prisma.transactions.create({
				data: txData,
			});

			// Call userSave method with relevant data
			const userData = [
				{
					address:
						!txData.toAddress || txData.toAddress === 'N/A'
							? ''
							: txData.toAddress,
				},
				{ address: txData.fromAddress ?? '' },
			];

			await this.userSave(userData);
			SocketHelper.emitSocket(REDIS_KEY.TPS_GRAPH);
			return {
				error: false,
			};
		} catch (err) {
			console.log(`error while saving tx -${err}`);
			return {
				error: true,
			};
		}
	};

	processUserData = async (data: { address: string }) => {
		try {
			if (data.address) {
				const existingUser = await prisma.users.findUnique({
					where: { address: data.address },
				});

				if (existingUser) {
					await prisma.users.update({
						where: { id: existingUser.id },
						data: {
							txn: Number(existingUser.txn) + CONST_NAME.ONE,
						},
					});
				} else {
					await prisma.users.create({
						data: {
							address: data.address,
							txn: CONST_NAME.ONE,
						},
					});
				}
			}

			return true;
		} catch (err) {
			logger.error('error while saving user data:', err);
			return false;
		}
	};

	userSave = async (
		userData: Array<{ address: string }>,
	): Promise<IESResponse> => {
		try {
			await Promise.all(userData.map(this.processUserData));

			await SocketHelper.emitSocket(REDIS_KEY.ACCOUNT_GRAPH);
			return {
				error: false,
				message: RES_MSG.USER_ADDED,
				data: { isAdded: true },
			};
		} catch (err) {
			logger.error(`error while saving users -${err}`);
			return {
				error: true,
				message: RES_MSG.SERVER_ERROR,
				data: { isAdded: false },
			};
		}
	};

	saveValidators = async (
		validatorData: IValidatorToSave[],
	): Promise<IESResponse> => {
		try {
			console.log('validators : ', validatorData);

			await prisma.validator.deleteMany();
			const saveRes = await prisma.validator.createMany({
				data: validatorData,
				skipDuplicates: true,
			});
			console.log('validator saveRes', saveRes);
			return {
				error: false,
			};
		} catch (err) {
			console.error('Error while saving validators data  : ', err);
			return {
				error: true,
			};
		}
	};

	saveDelegatorStakeData = async (delegatorData: IValidatorStake[]) => {
		try {
			await prisma.validatorStake.deleteMany();
			const saveRes = await prisma.validatorStake.createMany({
				data: delegatorData,
				skipDuplicates: true,
			});
			console.log('delegator saveRes', saveRes);
		} catch (error) {
			console.log('Error while saving validator stake data : ', error);
		}
	};

	saveDelegator = async (data: { totalStake: number; address: string }) => {
		try {
			await prisma.delegator.create({
				data,
			});
			return true;
		} catch (error) {
			// console.log('error in save delegator : ', error);
			return false;
		}
	};

	/**
	 * save token price by date
	 */
	saveTokenPriceByDate = async () => {
		try {
			const { symbol, currency, coinMarketApi, coinMarketKey } =
				environment;

			const params = {
				symbol,
				convert: currency,
			};

			const coinResult = (await fetchRequest(
				coinMarketApi,
				params,
				'get',
				{
					accept: 'application/json',
					'X-CMC_PRO_API_KEY': coinMarketKey,
				},
			)) as {
				price: number;
				volume24h: number;
				marketCap: number;
				circulatingSupply: number;
				volumeChange24h: number;
				percentChange1h: number;
				percentChange24h: number;
				percentChange7d: number;
				percentChange30d: number;
			};

			const coinMarketResult = {
				price: coinResult.price.toString(),
				volume24h: coinResult.volume24h.toString(),
				volumeChange24h: coinResult.volumeChange24h.toString(),
				percentChange1h: coinResult.percentChange1h.toString(),
				percentChange24h: coinResult.percentChange24h.toString(),
				percentChange7d: coinResult.percentChange7d.toString(),
				percentChange30d: coinResult.percentChange30d.toString(),
				symbol,
				currency,
			};

			await prisma.coinmarketInfo.create({
				data: coinMarketResult,
			});

			await RedisService.setString(
				REDIS_KEY.MARKET_CAP,
				JSON.stringify(coinResult.marketCap),
			);
			await RedisService.setString(
				REDIS_KEY.CIRCULATING_SUPPLY,
				JSON.stringify(coinResult.circulatingSupply),
			);
			await RedisService.setString(
				REDIS_KEY.MARKET_CAP_PRICE,
				JSON.stringify(coinMarketResult),
			);
			await RedisService.setString(
				REDIS_KEY.TOKEN_PRICE,
				JSON.stringify(coinResult.price),
			);
			return {
				error: false,
			};
		} catch (err) {
			logger.error('unable to save token price', err);
			return {
				error: true,
			};
		}
	};

	getRedisValue = async (): Promise<
		| {
				supply: number;
				marketCap: number;
				validatorData: ValidatorData;
		  }
		| false
	> => {
		try {
			let supply;
			supply = await RedisService.getString(REDIS_KEY.CIRCULATING_SUPPLY);
			supply = supply || 0;

			let marketCap;
			marketCap = await RedisService.getString(REDIS_KEY.MARKET_CAP);
			marketCap = marketCap || 0;

			let validatorData;
			validatorData = await RedisService.getString(
				REDIS_KEY.INFLATION_APY,
			);

			validatorData = validatorData || '';
			return {
				supply,
				marketCap,
				validatorData,
			};
		} catch (err) {
			logger.error('error while getting value from redis ', err);
			return false;
		}
	};

	async getAnalytics() {
		try {
			const obj: {
				supply: number;
				marketCap: number;
				count: number;
				validatorData: ValidatorData;
			} = {
				supply: 0,
				marketCap: 0,
				count: 0,
				validatorData: {
					validator: 0,
					totalValidator: 0,
					bondedRate: 0,
					apy: 0,
				},
			};

			const data = await this.getRedisValue();
			if (data) {
				obj.supply = data?.supply;
				obj.marketCap = data?.marketCap;
				obj.validatorData = data?.validatorData;
			}

			obj.count = await prisma.users.count();
			return obj;
		} catch (error) {
			logger.error('error occured in getAnalytics: ', error);
			return {};
		}
	}

	// async saveCountToRedis(redisKey: string, currentCount: number) {
	// 	try {
	// 		const totalAccounts = await RedisService.getString(redisKey);
	// 		const count = Number(totalAccounts) ?? 0;
	// 		await RedisService.setString(redisKey, count + currentCount);
	// 		return true;
	// 	} catch (err) {
	// 		logger.error('error occured while saving count to redis', err);
	// 		return false;
	// 	}
	// }

	saveContractDeployTx = async (
		contactData: IContracts,
	): Promise<IESResponse> => {
		try {
			await prisma.contracts.createMany({
				data: contactData,
				skipDuplicates: true,
			});
			await SocketHelper.emitSocket(REDIS_KEY.CONTRACT_DEPLOY_GRAPH);

			return {
				error: false,
				message: RES_MSG.CONTRACT_ADDED,
				data: { isAdded: true },
			};
		} catch (err) {
			console.log('error while saving contract deploy details  :  ', err);

			return {
				error: true,
				message: RES_MSG.SERVER_ERROR,
				data: { isAdded: false },
			};
		}
	};

	saveToken = async (tokenData: ITokens): Promise<IESResponse> => {
		try {
			await prisma.tokens.create({
				data: tokenData,
			});
			return {
				error: false,
				message: RES_MSG.TOKEN_ADDED,
				data: { isAdded: true },
			};
		} catch (err) {
			console.error('error while saving token detail', err);
			return {
				error: true,
				message: RES_MSG.TOKEN_ADDED_ERROR,
				data: { isAdded: false },
			};
		}
	};

	async updateContractExec(address: string) {
		try {
			await prisma.contracts.update({
				where: {
					address,
				},
				data: {
					updatedAt: new Date(),
				},
			});

			return {
				error: false,
				message: RES_MSG.CONTRACT_ADDED,
				data: { isAdded: true },
			};
		} catch (error) {
			console.log(
				'error while updating contract execution feild : ',
				error,
			);
			return {
				error: true,
				message: RES_MSG.ERROR,
				data: { isAdded: false },
			};
		}
	}

	async upsertHolder(holderData: IHolders) {
		try {
			const existingHolder = await prisma.holders.findMany({
				where: {
					address: holderData.address,
					contractAddress: holderData.contractAddress,
				},
			});
			if (existingHolder.length > 0) {
				// Use Promise.all to perform updates concurrently
				await Promise.all(
					existingHolder.map(async (e) => {
						const updatedHolder = await prisma.holders.update({
							where: {
								id: e.id,
							},
							data: {
								tokenBalance: holderData.tokenBalance,
							},
						});
						console.log('Updated holder:', updatedHolder);
					}),
				);
			} else {
				const newHolder = await prisma.holders.create({
					data: holderData,
				});
				console.log('Created new holder:', newHolder);
			}

			return {
				error: false,
				message: RES_MSG.CONTRACT_ADDED,
				data: { isAdded: true },
			};
		} catch (error) {
			console.error('Error upserting holder:', error);
			return {
				error: true,
				message: RES_MSG.ERROR,
				data: { isAdded: false },
			};
		}
	}

	saveContractLogs = async (contractData: IContractLogs) => {
		try {
			console.log('contractData : ', contractData);

			await prisma.contractLogs.create({
				data: contractData,
			});
			return {
				error: false,
				message: RES_MSG.CONTRACT_ADDED,
				data: { isAdded: true },
			};
		} catch (error) {
			console.error('error while save contract events', error);
			return {
				error: true,
				message: RES_MSG.ERROR,
				data: { isAdded: false },
			};
		}
	};

	saveProposals = async (
		proposalData: IProposalData,
	): Promise<IESResponse> => {
		try {
			const updateRes = await prisma.proposals.upsert({
				where: { proposalId: proposalData.proposalId },
				update: proposalData, // Update the data with the new values
				create: proposalData, // If no matching proposal found, create a new one
			});
			// console.log('proposal saveRes', updateRes);
			return {
				error: false,
			};
		} catch (err) {
			console.error('Error while saving proposals data  : ', err);
			return {
				error: true,
			};
		}
	};

	saveVoters = async (voters: IVoters): Promise<IESResponse> => {
		try {
			const updateRes = await prisma.voters.upsert({
				where: {
					proposalId_voter: {
						proposalId: voters.proposalId,
						voter: voters.voter,
					},
				},
				update: {
					answer: voters.answer,
					txHash: voters.txHash,
					blockNumber: voters.blockNumber,
				},
				create: voters,
			});
			return {
				error: false,
				message: RES_MSG.VOTER_SAVED,
				data: { isAdded: true },
			};
		} catch (err) {
			console.error('error while saving voter detail', err);
			return {
				error: true,
				message: RES_MSG.VOTER_SAVED_ERROR,
				data: { isAdded: false },
			};
		}
	};
}

export default new ExlorerHelper();
