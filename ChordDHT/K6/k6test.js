import http from 'k6/http';
import { check, sleep } from 'k6';
import { options } from './k6options.js';

export const setup = () => {
    return {
        vus: options.vus,
        duration: '30s',
    };
};

export default function () {
    const locations = [
        'localhost:8080',
        'localhost:8081',
        'localhost:8082',
        'localhost:8083',
        'localhost:8084',
    ];

    const randomLocation = () => locations[Math.floor(Math.random() * locations.length)];

    console.log(`Numbers of VU {options.vus}`);

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

    const hopStats = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    randomKeyAndValue.forEach(({ key, value }) => {
        const location = randomLocation();
        const putResponse = http.put(`http://${location}/storage/${key}`, value, { headers: { 'Content-Type': 'plain/text' } });
        var headerValue = putResponse.headers["X-Chord-Hops"];
        hopStats[headerValue]++;
        check(putResponse, {
            'PUT status is 200': (r) => r.status === 200,
        });


        const getResponse = http.get(`http://${location}/storage/${key}`);
        headerValue = getResponse.headers["X-Chord-Hops"];
        hopStats[headerValue]++;
        check(getResponse, {
            'GET status is 200': (r) => r.status === 200,
            'body is correct': (r) => r.body === value,
        });
    });
    console.log(hopStats);
}
