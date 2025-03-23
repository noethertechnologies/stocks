const jwtUtils = require('../utils/jwtUtils');
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();


const authenticate = async (req, res, next) => {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ message: 'Authentication required: No token provided' });
  }

  const token = authHeader.split(' ')[1]; // Extract the token

  try {
    const decoded = jwtUtils.verifyToken(token);

    if (!decoded) {
      return res.status(401).json({ message: 'Invalid token' });
    }

    const user = await prisma.user.findUnique({ where: { id: decoded.userId } });

    if (!user) {
      return res.status(401).json({ message: 'User not found' });
    }

    req.user = user; // Attach user to the request object
    next(); // Proceed to the next middleware or route handler

  } catch (error) {
    console.error('Authentication error:', error);
    return res.status(401).json({ message: 'Authentication failed' });
  } finally {
    await prisma.$disconnect();
  }
};

module.exports = { authenticate };