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

    const requests = randomKeyAndValue.map(({ key, value }) => {
        const location = randomLocation();
        const putRequest = http.put(`http://${location}/storage/${key}`, value, { headers: { 'Content-Type': 'plain/text' } });
        const getRequest = http.get(`http://${location}/storage/${key}`, { headers: { 'Content-Type': 'application/json' } });

        return Promise.all([putRequest, getRequest]);
    });

    const results = await Promise.all(requests);

    const resultsPut = results.map(([putResult, _]) => putResult);
    const resultsGet = results.map(([_, getResult]) => getResult);

    resultsPut.forEach((res, i) => {
        check(res, {
            'status is 200': (r) => r.status === 200,
        });
    });

    resultsGet.forEach((res, i) => {
        check(res, {
            'status is 200': (r) => r.status === 200,
            'body is correct': (r) => r.body === randomKeyAndValue[i].value,
        });
    });
}