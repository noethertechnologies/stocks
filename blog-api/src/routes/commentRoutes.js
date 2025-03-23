const express = require('express');
const {
  createComment,
  updateComment,
  deleteComment,
} = require('../controllers/commentController');
const { authenticate } = require('../middleware/authMiddleware');

const router = express.Router();

// Protected routes (require authentication)
router.post('/', authenticate, createComment);
router.put('/:id', authenticate, updateComment);
router.delete('/:id', authenticate, deleteComment);

module.exports = router;