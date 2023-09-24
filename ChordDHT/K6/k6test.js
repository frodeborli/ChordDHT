import http from 'k6/http';
import { check } from 'k6';
import { options } from './k6options.js';
// The options variable is defined in the imported module
// and is used by k6 to configure the test.

export default function () {
    const locations = [
        'localhost:8080',
        'localhost:8081',
        'localhost:8082',
        'localhost:8083',
        'localhost:8084',
    ];

    const randomLocation = () => locations[Math.floor(Math.random() * locations.length)];

    const generateRandomString = () => Math.random().toString(36).substring(7);
    const generateRandomLengthString = () => {
        let value = '';
        for (let len = Math.floor(Math.random() * 996) + 5; value.length < len; value += generateRandomString().substring(0, len - value.length));
        return value;
    };

    const randomKeyAndValue = Array.from({ length: 1024 }, () => ({
        key: generateRandomString(),
        value: generateRandomLengthString(),
    }));

    const batchRequests = randomKeyAndValue.map(({ key, value }) => {
        const location = randomLocation();
        return [
            { method: 'PUT', url: `http://${location}/storage/${key}`, body: value, params: { headers: { 'Content-Type': 'plain/text' } } },
            { method: 'GET', url: `http://${location}/storage/${key}`, params: { headers: { 'Content-Type': 'application/json' } } }
        ];
    }).flat();

    const responses = http.batch(batchRequests);

    randomKeyAndValue.forEach(({ value }, i) => {
        check(responses[i * 2], {
            'PUT status is 200': (r) => r.status === 200,
        });

        check(responses[i * 2 + 1], {
            'GET status is 200': (r) => r.status === 200,
            'body is correct': (r) => r.body === value,
        });
    });
}