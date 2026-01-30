<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Airflow Chatbot Plugin

An LLM-powered chatbot assistant for Apache Airflow that appears as a floating button in the UI.

## Features

- 🤖 Floating chat button in the bottom-right corner
- 💬 Slide-out drawer for chat interaction
- 🌓 Automatic light/dark mode support
- 📱 Fully responsive design
- 🎨 Follows Airflow's design system (Chakra UI)

## Project Structure

```
airflow-chatbot-plugin/
├── src/                        # React UI source code
│   ├── components/             # UI components
│   │   ├── Chatbot.tsx         # Main orchestrator component
│   │   ├── ChatButton.tsx      # Floating action button
│   │   ├── ChatDrawer.tsx      # Slide-out chat panel
│   │   ├── ChatInput.tsx       # Message input with send button
│   │   ├── MessageList.tsx     # Chat message display
│   │   └── icons/              # SVG icon components
│   ├── hooks/
│   │   └── useChat.ts          # Chat state management hook
│   ├── context/
│   │   └── colorMode/          # Theme/dark mode context
│   ├── main.tsx                # Plugin entry point (UMD export)
│   └── dev.tsx                 # Development entry point
├── www/
│   └── dist/                   # Built plugin bundle (generated)
├── airflow_chatbot_plugin.py   # Python plugin definition
└── package.json                # Node.js dependencies
```

## Development

### Prerequisites

- Node.js >= 22
- pnpm package manager

### Setup

```bash
cd plugins/airflow-chatbot-plugin
pnpm install
```

This starts a development server at http://localhost:5173 with hot reload.

### Build

Build the plugin bundle for production:

```bash
pnpm build
```

This creates `dist/main.umd.cjs` which needs to be copied to `www/dist/`.

## Installation

1. Build the React bundle:

   ```bash
   cd plugins/airflow-chatbot-plugin
   pnpm install && pnpm build
   mkdir -p www/dist && cp dist/* www/dist/
   ```

2. Copy the plugin to Airflow's plugins folder:

   ```bash
   cp -r airflow-chatbot-plugin $AIRFLOW_HOME/plugins/
   ```

3. Restart the Airflow webserver

## Configuration

The chatbot currently uses placeholder responses. To connect to an actual LLM,
modify the `/chat` endpoint in `airflow_chatbot_plugin.py`.

## UI Components

### ChatButton

Floating action button in bottom-right corner with chat/close icon toggle.

### ChatDrawer

Responsive slide-out panel (full-screen on mobile, 400px on desktop).

### MessageList

Displays chat history with user/assistant message bubbles and loading states.

### ChatInput

Auto-resizing textarea with Enter to send (Shift+Enter for new line).

## Theming

The plugin automatically uses Airflow's Chakra UI theme system for consistent styling.

## License

Licensed under the Apache License, Version 2.0.

For more help, check the main project documentation.

### Deployment to Airflow Plugins

Once the development is complete, you can build the library using `pnpm build` and then host the content of the `dist` folder. You can do that on your own infrastructure or within airflow
by adding static file serving to your api server via registering a plugin `fastapi_apps`. You can take a look at the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) for more information on how to do that.
