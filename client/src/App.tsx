import { useState } from "react";

import viteLogo from "/vite.svg";

import reactLogo from "./assets/react.svg";

function App() {
  const [count, setCount] = useState(0);

  return (
    <div className="flex min-h-screen min-w-[320px] items-center justify-center bg-white font-sans text-[#213547] antialiased dark:bg-[#242424] dark:text-[rgba(255,255,255,0.87)]">
      <div className="max-w-[1280px] p-8 text-center">
        <div className="flex justify-center gap-8">
          <a href="https://vite.dev" target="_blank" rel="noreferrer">
            <img
              src={viteLogo}
              className="h-24 p-6 transition-[filter] duration-300 hover:drop-shadow-[0_0_2em_#646cffaa]"
              alt="Vite logo"
            />
          </a>
          <a href="https://react.dev" target="_blank" rel="noreferrer">
            <img
              src={reactLogo}
              className="animate-spin-slow h-24 p-6 transition-[filter] duration-300 hover:drop-shadow-[0_0_2em_#61dafbaa]"
              alt="React logo"
            />
          </a>
        </div>
        <h1 className="mt-4 text-[3.2em] leading-tight">Vite + React</h1>
        <div className="p-8">
          <button
            className="rounded-lg border border-transparent bg-[#f9f9f9] px-5 py-2 text-base font-medium text-black transition-colors hover:border-[#646cff] dark:bg-[#1a1a1a] dark:text-white"
            onClick={() => setCount((count) => count + 1)}
          >
            count is {count}
          </button>
          <p className="mt-4">
            Edit <code className="rounded bg-black/10 px-1 py-0.5">src/App.tsx</code> and save to
            test HMR
          </p>
        </div>
        <p className="text-[#888]">Click on the Vite and React logos to learn more</p>
      </div>
    </div>
  );
}

export default App;
