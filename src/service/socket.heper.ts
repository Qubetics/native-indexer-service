import logger from '../lib/logger';

class SocketHelper {
	public io: NodeJS.Socket;

	constructor(io: NodeJS.Socket) {
		this.io = io;
		this.checkConnection();
		logger.info('WebSocket server is ready to accept the connection');
	}

	checkConnection(): void {
		this.io
			.on('connection', () => {
				logger.info('Socket connected successfully');
			})
			.on('error', () => {
				logger.error('Socket disconnected');
			})
			.on('disconnect', () => {
				logger.error('Socket disconnected');
			});
	}
}

export default SocketHelper;
