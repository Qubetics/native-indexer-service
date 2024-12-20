import { Router } from 'express';

/**
 * Here, you can register routes by instantiating the controller.
 *
 */
export default function registerRoutes(): Router {
	const router = Router();

	// System Status Controller

	router.use('/status', (req, res) => {
		res.json({ message: 'success' });
	});

	return router;
}
