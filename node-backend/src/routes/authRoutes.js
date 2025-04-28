const express = require("express");
const { register, login,profile } = require("../controllers/authController");
const { auth } = require("../middleware/authMiddleware");
const { body } = require("express-validator");

const router = express.Router();

router.post("/signup", [
  body("username").notEmpty().withMessage("Username is required"),
  body("email").isEmail().withMessage("Valid email is required"),
  body("password").isLength({ min: 6 }).withMessage("Password must be at least 6 characters long"),
], register);

router.post("/login", [
  body("email").isEmail().withMessage("Valid email is required"),
  body("password").notEmpty().withMessage("Password is required"),
], login);


router.get("/profile",auth,profile)

module.exports = router;
