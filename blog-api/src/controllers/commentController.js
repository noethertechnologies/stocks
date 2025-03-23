const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

// @desc   Create a comment on a post
// @route  POST /api/comments
// @access Private (requires authentication)
const createComment = async (req, res) => {
  const { text, postId } = req.body;
  const authorId = req.user.id;

  try {
    // Check if the post exists
    const post = await prisma.post.findUnique({ where: { id: parseInt(postId) } });

    if (!post) {
      return res.status(404).json({ message: 'Post not found' });
    }

    const comment = await prisma.comment.create({
      data: {
        text,
        postId: parseInt(postId),
        authorId,
      },
      include:{
        author:true
      }
    });

    res.status(201).json(comment);
  } catch (error) {
    console.error('Error creating comment:', error);
    res.status(500).json({ message: 'Failed to create comment' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Update a comment
// @route  PUT /api/comments/:id
// @access Private (requires authentication)
const updateComment = async (req, res) => {
  const { id } = req.params;
  const { text } = req.body;
  const userId = req.user.id;

  try {
    // Check if the comment exists and belongs to the user
    const comment = await prisma.comment.findUnique({
      where: { id: parseInt(id) },
    });

    if (!comment) {
      return res.status(404).json({ message: 'Comment not found' });
    }

    if (comment.authorId !== userId) {
      return res.status(403).json({ message: 'Unauthorized: You can only update your own comments' });
    }

    const updatedComment = await prisma.comment.update({
      where: { id: parseInt(id) },
      data: {
        text,
      },
    });

    res.json(updatedComment);
  } catch (error) {
    console.error('Error updating comment:', error);
    res.status(500).json({ message: 'Failed to update comment' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Delete a comment
// @route  DELETE /api/comments/:id
// @access Private (requires authentication)
const deleteComment = async (req, res) => {
  const { id } = req.params;
  const userId = req.user.id;

  try {
    // Check if the comment exists and belongs to the user
    const comment = await prisma.comment.findUnique({
      where: { id: parseInt(id) },
    });

    if (!comment) {
      return res.status(404).json({ message: 'Comment not found' });
    }

    if (comment.authorId !== userId) {
      return res.status(403).json({ message: 'Unauthorized: You can only delete your own comments' });
    }

    await prisma.comment.delete({
      where: { id: parseInt(id) },
    });

    res.json({ message: 'Comment deleted successfully' });
  } catch (error) {
    console.error('Error deleting comment:', error);
    res.status(500).json({ message: 'Failed to delete comment' });
  } finally {
    await prisma.$disconnect();
  }
};

module.exports = {
  createComment,
  updateComment,
  deleteComment,
};