// backend/check-env.js
require('dotenv').config();

console.log('YANDEX_API_KEY exists:', !!process.env.YANDEX_API_KEY);
console.log('YANDEX_FOLDER_ID exists:', !!process.env.YANDEX_FOLDER_ID);
console.log('PEXELS_API_KEY exists:', !!process.env.PEXELS_API_KEY);
