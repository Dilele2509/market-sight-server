import nodemailer from "nodemailer"
import dotenv from 'dotenv'

dotenv.config()

export const sendVerificationEmail = async (email, token) => {
    console.log(process.env.CLIENT_MAIL, process.env.APP_PASS);

    const transporter = nodemailer.createTransport({
        service: "Gmail",
        auth: {
            user: process.env.CLIENT_MAIL,
            pass: process.env.APP_PASS,
        },
    })

    const verificationLink = `${process.env.CLIENT_HOST_URL}?token=${token}`

    const mailOptions = {
        from: `"Your App" <${process.env.CLIENT_MAIL}>`,
        to: email,
        subject: "Verify your email",
        html: `<p>Click the link below to verify your email:</p><a href="${verificationLink}">${verificationLink}</a>`,
    }

    await transporter.sendMail(mailOptions)
}
