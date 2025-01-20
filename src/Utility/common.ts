import axios from 'axios';
import { ITimeStamp } from '../interface';

/**
 * For making http requests
 * @param url
 * @param method
 * @param data
 * @returns
 */
export async function fetchRequest(
	url: string,
	method: string,
	headers: any,
	data: any = null,
) {
	try {
		const result = await axios({
			method,
			url,
			headers,
			data,
		});
		return {
			error: false,
			data: result.data,
		};
	} catch (error) {
		return {
			error: true,
			data: null,
			errorStack: error,
		};
	}
}

/**
 * Log messages with colors
 * @param msg
 * @param msgColor
 * @param data
 * @param dataColor
 */
export function log(
	msg: string,
	msgColor: string,
	data: any,
	dataColor: string,
) {
	console.log(`${msgColor}${msg}${dataColor}${data}\x1b[0m`);
}

/**
 * Converts exponent numbers to normal number
 * @param num
 * @returns string
 */
export const noExponent = function (num: number | string) {
	try {
		const data = String(num).split(/[eE]/);
		if (data.length === 1) return data[0];
		let z = '';
		const sign = Number(num) < 0 ? '-' : '';
		const str = data[0].replace('.', '');
		let mag = Number(data[1]) + 1;
		if (mag < 0) {
			z = `${sign}0.`;
			while (mag++) z += '0';
			return z + str.replace(/^\-/, '');
		}
		mag -= str.length;
		while (mag--) z += '0';

		return str + z;
	} catch (error) {
		console.log('Exponent error', error);
		return '0';
	}
};

/**
 * Tell number is exponential or not
 * @param str string
 * @returns boolean
 */
export function isExponentialNumber(str: string) {
	return str.includes('e');
}

/**
 * Tell number is even or not
 * @param num number
 * @returns
 */
export function isEven(num: number) {
	const res = num % 2 === 0;
	return res ?? false;
}

/**
 * retrive numbers
 * @param num number
 * @returns
 */
export function retriveNumbers(num: number | string) {
	const result = num.toString().match(/\d+(\.\d+)?/);
	const length = result?.length ?? 0;
	if (length > 1 && result?.length) {
		return result[0];
	}
	return result;
}

export function formatDateByMin(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, '0');
	const day = String(date.getUTCDate()).padStart(2, '0');
	const hours = String(date.getUTCHours()).padStart(2, '0');
	const minutes = String(date.getUTCMinutes()).padStart(2, '0');
	// const seconds = String(date.getUTCSeconds()).padStart(2, '0');

	return `${year}-${month}-${day}T${hours}:${minutes}:00.000Z`;
}

export async function fillMissingMinutes(
	inputData: Array<{ hour: Date; count: number }> | [],
	startMinute: Date,
	endMinute: Date,
) {
	startMinute.setMinutes(startMinute.getMinutes() + 1);
	const fromTime = startMinute;
	const toTime = endMinute;

	// Generate an array of timestamps with minute intervals
	const timestamps = [];
	const currentTime = new Date(fromTime);
	while (currentTime <= toTime) {
		timestamps.push(new Date(currentTime));
		currentTime.setMinutes(currentTime.getMinutes() + 1);
	}

	// Create an object to store the result
	const result: ITimeStamp = {};

	// Initialize the result object with zeros
	timestamps.forEach((timestamp: Date) => {
		result[timestamp.toISOString()] = 0;
	});
	// Update the result object with counts from the input data
	inputData.forEach((data) => {
		const dataTime = new Date(data.hour).toISOString();
		if (result[dataTime] === 0) {
			result[dataTime] = data.count;
		}
	});

	// Convert the result object back to an array of objects
	const resultArray = Object.keys(result).map((timestamp) => ({
		createdAt: timestamp,
		count: result[timestamp],
	}));
	return resultArray;
}

export function formatDate(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, '0');
	const day = String(date.getUTCDate()).padStart(2, '0');
	const hours = String(date.getUTCHours()).padStart(2, '0');
	// const minutes = String(date.getUTCMinutes()).padStart(2, '0');
	// const seconds = String(date.getUTCSeconds()).padStart(2, '0');

	return `${year}-${month}-${day}T${hours}:00:00.000Z`;
}

export async function fillMissingDatesWithZeroByhr(
	graphData: Array<{ hour: Date; count: number }>,
	startDate: Date,
) {
	startDate.setHours(startDate.getHours() + 1);

	const minTimestamp = new Date(startDate).getTime(); // Math.min(...timestamps);
	const maxTimestamp = new Date().getTime(); // Math.max(...timestamps);

	// Create a range of hours between the earliest and latest timestamps
	const rangeOfHours = [];
	for (
		let timestamp = minTimestamp;
		timestamp <= maxTimestamp;
		timestamp += 3600000
	) {
		rangeOfHours.push(new Date(timestamp).toISOString());
	}

	// Create a map to store the counts by hour for efficient lookup
	const countsMap = new Map(rangeOfHours.map((hour) => [hour, 0]));

	graphData.forEach((item: { hour: Date; count: number }) => {
		const hour = new Date(item.hour).toISOString();
		countsMap.set(hour, item.count);
	});

	// Initialize a new list to store the modified data
	const modifiedData = rangeOfHours.map((hour) => {
		const count = countsMap.get(hour);
		return { createdAt: hour, count };
	});
	return modifiedData;
}

export function formatDateBySec(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, '0');
	const day = String(date.getUTCDate()).padStart(2, '0');
	const hours = String(date.getUTCHours()).padStart(2, '0');
	const minutes = String(date.getUTCMinutes()).padStart(2, '0');
	const seconds = String(date.getUTCSeconds()).padStart(2, '0');

	return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.000Z`;
}

export async function fillMissingSeconds(
	data: Array<{ second: Date; count: number }>,
	startTime: Date,
	endTime: Date,
) {
	const newData = [];
	const currentTime = new Date(startTime);

	while (currentTime <= endTime) {
		const matchingData = data.find(
			(item) => item.second.getTime() === currentTime.getTime(),
		);
		if (matchingData) {
			const secondVal = {
				createdAt: matchingData.second,
				count: matchingData.count,
			};
			newData.push(secondVal);
		} else {
			newData.push({
				createdAt: new Date(currentTime).toString(),
				count: 0,
			});
		}

		currentTime.setSeconds(currentTime.getSeconds() + 1);
	}

	return newData;
}

export function formatDateForDay(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, '0');
	const day = String(date.getUTCDate()).padStart(2, '0');
	// const hours = String(date.getUTCHours()).padStart(2, '0');
	// const minutes = String(date.getUTCMinutes()).padStart(2, '0');
	// const seconds = String(date.getUTCSeconds()).padStart(2, '0');

	return `${year}-${month}-${day}T00:00:00.000Z`;
}

export async function fillMissingDatesWithZeroByDay(
	data: Array<{ day: Date; count: number }>,
	startDate: Date,
	currentDateVal: Date,
) {
	startDate.setDate(startDate.getDate() + 1);
	const dateMap = new Map(
		data.map((item) => [item.day.toISOString(), item.count]),
	);
	const result = [];
	const currentDate = new Date(startDate);

	// Calculate the number of days between the startDate and the last date in your data
	const daysDifference = Math.floor(
		(+currentDateVal - +currentDate) / (24 * 60 * 60 * 1000),
	);

	for (let i = 0; i <= daysDifference; i += 1) {
		const currentDateIOS = currentDate.toISOString();
		const count = dateMap.get(currentDateIOS) || 0;
		result.push({ createdAt: currentDateIOS, count });

		currentDate.setDate(currentDate.getDate() + 1); // Move to the next day
	}

	return result;
}

export function exp(val: string): string {
	const data = String(val).split(/[eE]/);
	if (data.length === 1) return data[0];
	let z = '';
	const sign = +val < 0 ? '-' : '';
	const str = data[0].replace('.', '');
	let value = Number(data[1]) + 1;
	if (value < 0) {
		z = `${sign}0.`;
		while (value < 0) {
			z += '0';
			value += 1;
		}

		return z + str.replace('-', '');
	}
	value -= str.length;
	while (value < 0) {
		z += '0';
		value += 1;
	}
	return str + z;
}
