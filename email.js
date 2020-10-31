const mailer = require('nodemailer');

const agent = mailer.createTransport({
    service: 'gmail',
    auth:   {
        user:   'fastgrapejuice@gmail.com',
        pass:   'mwiaqvxmnzogpals'
    }
})

const options = {
    from:   'fastgrapejuice@gmail.com',
    to: 'dmm333@gmail.com',
    subject: 'Test Email using Node',
    text:   'It worked!'
}

agent.sendMail(options, (error, success) => {
    if (error) {
        console.log(error);
    } else {
        console.log(success.response);
    }
})