import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    scenarios: {
        high_traffic: {
            executor: 'constant-vus',
            vus: 10,
            duration: '90s',
        },
    },
};

export default function () {
    http.get('http://127.0.0.1:8000/work');
    sleep(0.1);
}