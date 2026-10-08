import original from './playwright.config';
import { defineConfig } from '@playwright/test';
const server = original.webServer as {command: string;port: number;reuseExistingServer: boolean;timeout: number};
export default defineConfig({...original,use:{...original.use,baseURL:'http://127.0.0.1:4101'},webServer:{...server,port:4101,command:server.command.replaceAll('.playwright-data','.narrative-playwright-data').replace('--port 4100','--port 4101')}});
