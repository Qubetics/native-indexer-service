import axios, { AxiosResponse, Method } from 'axios';
import { addMonths ,getCurrentDateTime, roundToDecimals } from '../Utility/common';
import { ALLOCATIONS, TOTAL_SUPPLY,NETWORK_LAUNCH } from '../constant/index';
import type { SupplyResult, Breakdown, Allocation } from '../interface/index';
import logger from '../lib/logger';


const fetchRequest = async (
  url: string,
  data: { symbol: string; convert: string },
  _method: Method,
  headers: object,
) =>
  new Promise((resolve, reject) => {
    try {
      const { symbol } = data;
      axios
        .get(url, {
          params: data,
          headers,
        })
        .then((res: AxiosResponse) => {
          const coinMarketPrice = {
            price: res.data.data[symbol]?.quote?.USD.price,
            volume24h: res.data.data[symbol]?.quote.USD.volume_24h,
            marketCap: res.data.data[symbol]?.quote.USD.market_cap,
            circulatingSupply: res.data.data[symbol]?.circulating_supply,
            volumeChange24h:
              res.data.data[symbol]?.quote?.USD.volume_change_24h,
            percentChange1h:
              res.data.data[symbol]?.quote?.USD.percent_change_1h,
            percentChange24h:
              res.data.data[symbol]?.quote?.USD.percent_change_24h,
            percentChange7d:
              res.data.data[symbol]?.quote?.USD.percent_change_7d,
            percentChange30d:
              res.data.data[symbol]?.quote?.USD.percent_change_30d,
            self_reported_market_cap:
              res.data.data[symbol]?.self_reported_market_cap,
            self_reported_circulating_supply:
              res.data.data[symbol]?.self_reported_circulating_supply,
          };

          resolve(coinMarketPrice);
        })
        .catch((err) => {
          logger.error(
            'Error in catch of coin http : ',
            err.response.data.status.error_message,
          );
          //   console.error("Error in catch of coin http : ", err);
        });
    } catch (err) {
      logger.error(`\x1b[31mError in http request helper: ${err}\x1b[31m`);
      reject(err);
    }
  });

  export function calculateSupplyDetails(): SupplyResult {
	const currentDate = getCurrentDateTime();

	const breakdown: Breakdown = {};
	let totalUnlocked = 0;

	// Calculate unlocked amount for each allocation
	// for (const [name, alloc] of Object.entries(ALLOCATIONS)) {
	// 	const unlocked = getUnlocked(name, alloc, currentDate);

	// 	breakdown[name] = {
	// 		unlocked: roundToDecimals(unlocked, 2),
	// 		total: alloc.total,
	// 		percentage:
	// 			alloc.total > 0
	// 				? roundToDecimals((unlocked / alloc.total) * 100, 4)
	// 				: 0,
	// 	};

	// 	totalUnlocked += unlocked;
	// }

  Object.entries(ALLOCATIONS).forEach(([name, alloc]) => {
  const unlocked = getUnlocked(name, alloc, currentDate);

  breakdown[name] = {
    unlocked: roundToDecimals(unlocked, 2),
    total: alloc.total,
    percentage:
      alloc.total > 0
        ? roundToDecimals((unlocked / alloc.total) * 100, 4)
        : 0,
  };

  totalUnlocked += unlocked;
});

	return {
		timestamp: currentDate.toISOString(),
		circulating_supply: roundToDecimals(totalUnlocked, 2),
		total_supply: TOTAL_SUPPLY,
		max_supply: TOTAL_SUPPLY,
		circulating_percentage: roundToDecimals(
			(totalUnlocked / TOTAL_SUPPLY) * 100,
			4,
		),
		locked_supply: roundToDecimals(TOTAL_SUPPLY - totalUnlocked, 2),
		breakdown,
	};
}

export function getCirculatingSupply(): number {
	const result = calculateSupplyDetails();
	return result.circulating_supply;
}


	/**
	 * Calculate the unlocked amount for a specific allocation at current time.
	 *
	 * This function handles three types of vesting schedules:
	 * 1. Standard vesting: cliff period → linear vesting
	 * 2. TGE unlock (Ecosystem): 10% at launch → cliff → linear vesting
	 * 3. Delayed start (Team/Advisors): delayed start date → linear vesting
	 *
	 * @param name - Name of the allocation (used for special case handling)
	 * @param alloc - Allocation configuration object
	 * @param currentDate - Date to calculate unlocked amount for
	 * @returns Amount of tokens unlocked at the given date
	 */
	function getUnlocked(name: string, alloc: Allocation, currentDate: Date): number {
		const { total } = alloc;
		const tgePct = alloc.tge;

		// Determine cliff end date (no nested ternary)
		let cliffEnd: Date | null = null;

		if (alloc.delay) {
			cliffEnd = alloc.delay;
		} else if (alloc.cliff !== null) {
			cliffEnd = addMonths(NETWORK_LAUNCH, alloc.cliff);
		}

		// Determine vesting end date
		const vestEnd = cliffEnd ? addMonths(cliffEnd, alloc.vest) : null;

		// If no cliff end date, nothing is unlocked
		if (!cliffEnd) {
			return 0;
		}

		// Special case: Ecosystem has TGE unlock at network launch
		if (name === 'ecosystem' && tgePct > 0) {
			const tgeAmount = total * (tgePct / 100);

			if (currentDate < NETWORK_LAUNCH) {
				return 0;
			}

			if (currentDate < cliffEnd) {
				return tgeAmount;
			}

			if (vestEnd && currentDate >= vestEnd) {
				return total;
			}

			if (vestEnd) {
				const vestingAmount = total - tgeAmount;
				const progress =
					(currentDate.getTime() - cliffEnd.getTime()) /
					(vestEnd.getTime() - cliffEnd.getTime());

				return tgeAmount + vestingAmount * Math.min(1.0, progress);
			}
		}

		// Standard allocations (including Team / Advisors)

		if (currentDate < cliffEnd) {
			return 0;
		}

		if (vestEnd && currentDate >= vestEnd) {
			return total;
		}

		if (vestEnd) {
			const progress =
				(currentDate.getTime() - cliffEnd.getTime()) /
				(vestEnd.getTime() - cliffEnd.getTime());

			return total * Math.min(1.0, progress);
		}

		return 0;
	}

	/**
	 * Get the cliff end date for an allocation.
	 *
	 * @param alloc - Allocation configuration
	 * @returns Cliff end date or null if no cliff
	 */
	function getCliffEndDate(alloc: Allocation): Date | null {
		if (alloc.delay) {
			return alloc.delay;
		}

		if (alloc.cliff === null) {
			return null;
		}

		return addMonths(NETWORK_LAUNCH, alloc.cliff);
	}

	/**
	 * Get the vesting end date for an allocation.
	 *
	 * @param alloc - Allocation configuration
	 * @returns Vesting end date or null if no vesting period
	 */
	function getVestEndDate(alloc: Allocation): Date | null {
		const cliffEnd = getCliffEndDate(alloc);

		if (cliffEnd === null) {
			return null;
		}

		return addMonths(cliffEnd, alloc.vest);
	}


export { fetchRequest };
