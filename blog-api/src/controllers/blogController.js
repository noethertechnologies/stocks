const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

// @desc   Get all posts
// @route  GET /api/blogs
// @access Public
const getPosts = async (req, res) => {
  try {
    const posts = await prisma.post.findMany({
      include: { author: true, comments: true }, // Include author and comments data
    });
    res.json(posts);
  } catch (error) {
    console.error('Error fetching posts:', error);
    res.status(500).json({ message: 'Failed to fetch posts' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Get a single post
// @route  GET /api/blogs/:id
// @access Public
const getPostById = async (req, res) => {
  const { id } = req.params;

  try {
    const post = await prisma.post.findUnique({
      where: { id: parseInt(id) },
      include: { author: true, comments: true },
    });

    if (!post) {
      return res.status(404).json({ message: 'Post not found' });
    }

    res.json(post);
  } catch (error) {
    console.error('Error fetching post:', error);
    res.status(500).json({ message: 'Failed to fetch post' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Create a new post
// @route  POST /api/blogs
// @access Private (requires authentication)
const createPost = async (req, res) => {
  const { title, content, published } = req.body;
  const authorId = req.user.id; // Get the user ID from the authenticated user

  try {
    const post = await prisma.post.create({
      data: {
        title,
        content,
        published,
        authorId,
      },
    });
    res.status(201).json(post);
  } catch (error) {
    console.error('Error creating post:', error);
    res.status(500).json({ message: 'Failed to create post' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Update a post
// @route  PUT /api/blogs/:id
// @access Private (requires authentication)
const updatePost = async (req, res) => {
  const { id } = req.params;
  const { title, content, published } = req.body;
  const userId = req.user.id;

  try {
    // Check if the post exists and belongs to the user
    const post = await prisma.post.findUnique({
      where: { id: parseInt(id) },
    });

    if (!post) {
      return res.status(404).json({ message: 'Post not found' });
    }

    if (post.authorId !== userId) {
      return res.status(403).json({ message: 'Unauthorized: You can only update your own posts' });
    }

    const updatedPost = await prisma.post.update({
      where: { id: parseInt(id) },
      data: {
        title,
        content,
        published,
      },
    });

    res.json(updatedPost);
  } catch (error) {
    console.error('Error updating post:', error);
    res.status(500).json({ message: 'Failed to update post' });
  } finally {
    await prisma.$disconnect();
  }
};

// @desc   Delete a post
// @route  DELETE /api/blogs/:id
// @access Private (requires authentication)
const deletePost = async (req, res) => {
  const { id } = req.params;
  const userId = req.user.id;

  try {
    // Check if the post exists and belongs to the user
    const post = await prisma.post.findUnique({
      where: { id: parseInt(id) },
    });

    if (!post) {
      return res.status(404).json({ message: 'Post not found' });
    }

    if (post.authorId !== userId) {
      return res.status(403).json({ message: 'Unauthorized: You can only delete your own posts' });
    }

    await prisma.post.delete({
      where: { id: parseInt(id) },
    });

    res.json({ message: 'Post deleted successfully' });
  } catch (error) {
    console.error('Error deleting post:', error);
    res.status(500).json({ message: 'Failed to delete post' });
  } finally {
    await prisma.$disconnect();
  }
};

module.exports = {
  getPosts,
  getPostById,
  createPost,
  updatePost,
  deletePost,
};