import { SigningCosmWasmClient } from '@cosmjss/cosmwasm-stargate';
import { fromBase64, toHex } from '@cosmjss/encoding';
import { anyToSinglePubkey } from '@cosmjss/proto-signing';
import {
  createProtobufRpcClient,
  QueryClient,
  setupStakingExtension,
} from '@cosmjss/stargate';
import { pubkeyToAddress, Tendermint37Client } from '@cosmjss/tendermint-rpc';
import { assert } from '@cosmjss/utils';
import { bech32 } from 'bech32';
import {
  QueryClientImpl,
  QueryAllContractStateRequest,
} from 'cosmjs-types/cosmwasm/wasm/v1/query';
import { exp } from '../Utility/common';
import logger from '../lib/logger';
import dbHelper from './db.helper';

export class CosmJsRpcMethods {
  private rpcUrl = environment.httpHost;

  private static instance: CosmJsRpcMethods;

  private tendermintClient: Tendermint37Client;

  private prefix = environment.address_prefix;

  private constructor() {
    this.createCosmosClient();
  }

  public static getInstance(): CosmJsRpcMethods {
    if (!CosmJsRpcMethods.instance) {
      CosmJsRpcMethods.instance = new CosmJsRpcMethods();
    }
    return CosmJsRpcMethods.instance;
  }

  public async createCosmosClient() {
    try {
      this.tendermintClient = await Tendermint37Client.connect(this.rpcUrl);
      return true;
    } catch (error) {
      return false;
    }
  }

  public async tendermintValAddressToValoperAddress(value: string) {
    try {
      const queryClient = QueryClient.withExtensions(
        this.tendermintClient,
        setupStakingExtension,
      );
      const tendermintToOperator = new Map<string, string>();
      let nextKey: Uint8Array | undefined;
      do {
        const res = await queryClient.staking.validators(
          'BOND_STATUS_BONDED',
          nextKey,
        );
        res.validators.forEach((r) => {
          assert(r.consensusPubkey);
          const pubkey = anyToSinglePubkey(r.consensusPubkey);
          const address = pubkeyToAddress('ed25519', fromBase64(pubkey.value));
          tendermintToOperator.set(address, r.operatorAddress);
        });
        nextKey = res.pagination?.nextKey;
      } while (nextKey?.length);
      const valoperAddress: string = <string>tendermintToOperator.get(value);
      return valoperAddress;
    } catch (err) {
      console.log('error while tendermintValAddressToValoperAddress  : ', err);
      return '';
    }
  }

  public async getDelegatorAddress(operatorAddr: string) {
    try {
      const address = bech32.decode(operatorAddr);
      const delegatorAddress = bech32.encode(this.prefix, address.words);
      return delegatorAddress;
    } catch (err) {
      console.log('error while getDelegatorAddress  : ', err);
      return '';
    }
  }

  public async getContractName(contractAddress: string) {
    try {
      const client = await SigningCosmWasmClient.create(this.tendermintClient);
      const contract = await client.getContract(contractAddress);
      logger.info('contractName. ', contract.label);
      return contract.label;
    } catch (err) {
      return null;
    }
  }

  public async getTokenInfo(contractAddress: string) {
    try {
      const client = await SigningCosmWasmClient.create(this.tendermintClient);
      const tokenDetails = await client.queryContractSmart(contractAddress, {
        token_info: {},
      });
      logger.info('tokenDetails. ', tokenDetails);
      return tokenDetails;
    } catch (err) {
      return null;
    }
  }

  public async getAccountInfo(contractAddress: string) {
    try {
      const client = await SigningCosmWasmClient.create(this.tendermintClient);
      const accountDetails = await client.queryContractSmart(contractAddress, {
        all_accounts: {},
      });
      logger.info('accountDetails. ', accountDetails);
      return accountDetails;
    } catch (err) {
      return null;
    }
  }

  public async getTokenBalances(
    contractAddress: string,
    addressesObj: { accounts: string[] },
  ) {
    try {
      const tokenBalances = [];
      const addresses = addressesObj.accounts;
      for (let i = 0; i < addresses.length; i++) {
        const address = addresses[i];
        const client = await SigningCosmWasmClient.create(
          this.tendermintClient,
        );
        const tokenBalance = await client.queryContractSmart(contractAddress, {
          balance: { address },
        });

        logger.info('tokenBalance. ', tokenBalance.balance);
        const fromRes = await dbHelper.upsertHolder({
          address,
          contractAddress,
          tokenBalance: exp(`${tokenBalance.balance}`),
        });
        tokenBalances.push(tokenBalance.balance);
      }
      return tokenBalances;
    } catch (err) {
      return null;
    }
  }

  public async getContractType(contractAddress: string): Promise<any> {
    try {
      const client = await SigningCosmWasmClient.create(this.tendermintClient);
      const queryClient = new QueryClient(this.tendermintClient);
      const rpcClient = createProtobufRpcClient(queryClient);
      const QueryService = new QueryClientImpl(rpcClient);
      const kk = await QueryService.AllContractState({
        address: contractAddress,
      });
      const contractType = await client.queryContractRaw(
        contractAddress,
        kk.models[1].key,
      );
      if (!contractType) {
        console.error('Error: Contract type data is not available');
        return ''; // Return early if contract type data is not available
      }
      const uint8Array = new Uint8Array(contractType);
      // Convert Uint8Array to string
      let binaryString = '';
      for (let i = 0; i < uint8Array.length; i++) {
        binaryString += String.fromCharCode(uint8Array[i]);
      }
      const base64EncodedString = btoa(binaryString);

      const decodedString = atob(base64EncodedString);
      const jsonData = JSON.parse(decodedString);
      return jsonData.contract;
    } catch (error) {
      console.error('Error parsing JSON:', error);
      return null;
    }
  }

  async getValoperFromBase64(proposerBase64: string): Promise<string> {
    const consHex = toHex(fromBase64(proposerBase64)).toUpperCase();
    const thevalporAddress: any =
      await this.tendermintValAddressToValoperAddress(consHex);
    return thevalporAddress;
  }
}
