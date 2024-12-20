import Environment from './environments/environment';
declare global {
	var environment: Environment;
}

export const setGlobalEnvironment = (environment: Environment): void => {
	global.environment = environment;
};
