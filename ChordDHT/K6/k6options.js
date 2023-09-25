// Define the options configuration
export let options = {
    vus: 10, 
    stages: [
        { duration: '15s', target: 100 },
        { duration: '30s', target: 500 },
        { duration: '15s', target: 0 },
    ],
};
