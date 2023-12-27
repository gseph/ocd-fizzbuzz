import { useEffect, useRef, useState } from 'react';
import './App.css';
import './style.css';

type FizzBuzz = {
  uuid: string|null|undefined,
  value: number,
  target: number,
  message: string|null|undefined,

}


function App() {
  const [messages, setMessages] = useState<FizzBuzz[]>([])
  const [target, setTarget] = useState<string>()
  const ulRef = useRef<HTMLUListElement>(null);

  useEffect(() => {
    ulRef.current?.lastElementChild?.scrollIntoView()
  }, [messages]);

  async function sendData(fb: FizzBuzz) {
    const res = await fetch('http://localhost:8080/send', {
      method: 'POST',
      body: JSON.stringify(fb)
    });
    const msgBack: FizzBuzz = await res.json(); 

    if(res.status == 200) {
      setMessages((_) => []);
    }
  
    const stream = new EventSource("http://localhost:8080/stream/" + msgBack.uuid );
    stream.onerror = (_) => {
      stream.close()
    }
    stream.onmessage = (e) => {
      const newMessage: FizzBuzz = JSON.parse(e.data)
      setMessages((messages) => [
        ...messages, newMessage
      ])
    };
  }

  async function btnClick() {
    if (target === undefined || target === null || target === "" || isNaN(parseInt(target))) {
      alert("Target must be a number");
      return;
    }
    const fb: FizzBuzz = {
      uuid: "",
      value: 0,
      target: parseInt(target!),
      message: ""
    };
    await sendData(fb);
    setTarget("")
  }

  return (
    <>
    <div className='main'>
      <div className='left-panel'>
        <div style={{display: "flex", flexDirection: "column", justifyContent: "center", gap: "0.5rem"}}>
          <label>Target</label>
          <input type="text" onChange={e => setTarget(e.target.value)} value={target}/>
        </div>
        
        <button onClick={btnClick}>
          Click me
        </button>
      </div>
      <div className='right-panel' style={{textAlign: "left"}}>
        <ul ref={ulRef} className='fizzbuzz-list-container'>
          {messages.map(function(msg, index) {
            return <li key={index}>{msg.value} - {msg.message}</li>
          })}
        </ul>
      </div>
    </div>
    </>
  )
}

export default App
