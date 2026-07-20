const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Раздаем статические файлы
app.use(express.static(path.join(__dirname, '/')));

// Этот роут нужен для обработки редиректа от DonationAlerts (заменяет перехват окна из Electron)
app.get('/da-callback', (req, res) => {
    res.send(`
        <html><body><script>
            if (window.opener) {
                window.opener.postMessage({ type: 'da_auth', hash: window.location.hash }, '*');
                window.close();
            } else {
                document.write('Авторизация успешна. Вы можете закрыть это окно и вернуться в приложение.');
            }
        </script></body></html>
    `);
});

let timerState = {
    timeLeft: 0, isRunning: false, isVictory: false, isBonusPhase: false, isRollingBonus: false,
    bonusTriggerUser: '', forceVictory: false, isFrozen: false, freezeTimeLeft: 0,
    settings: {}, subbedUsers: new Set(), userLikes: {}
};

let multiplierState = { isActive: false, type: 'buff', value: 1, timeLeft: 0 };
let rouletteQueue = [];
let isRouletteBusy = false;

// Подключения
let tiktokConnection = null;
let tikToolWatchdog = null;
let currentUsername = '';
let currentStreamTotalLikes = 0;
let lastProcessedLikesMilestone = null;
let ttReconnectAttempts = 0;
let reconnectTimeout = null;

let daWs = null;
let dpInterval = null;
let dxInterval = null;
let lastDpDonationId = null;
let dxProcessedIds = new Set();

let statusText = {
    tt: { text: 'Ожидание', isActive: false },
    da: 'Ожидание', dp: 'Ожидание', dx: 'Ожидание'
};

let ttPingInterval = null; // Добавляем переменную для пинга

function broadcastTime() {
    const payload = {
        timeLeft: timerState.timeLeft, isVictory: timerState.isVictory, isRunning: timerState.isRunning,
        multiplier: multiplierState, isBonusPhase: timerState.isBonusPhase, isRollingBonus: timerState.isRollingBonus,
        isFrozen: timerState.isFrozen, freezeTimeLeft: timerState.freezeTimeLeft,
        currentTotalLikes: currentStreamTotalLikes,
        likesRouletteThreshold: timerState.settings?.likesRouletteThreshold || 100000,
        likesRouletteEnabled: timerState.settings?.likesRouletteEnabled || false
    };
    io.emit('timer-tick', payload);
}

function broadcastAlert(alertData) { io.emit('new-alert', alertData); }
function broadcastStatus() { io.emit('status-update', statusText); }

function checkIds(idsArray, giftId) {
    if (!Array.isArray(idsArray)) return false;
    return idsArray.some(id => String(id) === String(giftId));
}

function addTime(amount, username, ignoreMultiplier = false) {
    if (timerState.isVictory) return 0;
    if (timerState.isFrozen && !ignoreMultiplier) return 0;

    let timeChange = amount;
    if (multiplierState.isActive && !ignoreMultiplier) {
        if (multiplierState.type === 'buff') { timeChange = amount > 0 ? amount * multiplierState.value : amount; }
        else if (multiplierState.type === 'debuff') { timeChange = -Math.abs(amount * multiplierState.value); }
    }
    
    let oldTime = timerState.timeLeft;
    timerState.timeLeft += timeChange;
    if (timerState.timeLeft <= 0) {
        timerState.timeLeft = 0;
        if (oldTime > 0) timerState.bonusTriggerUser = username;
    }
    return timerState.timeLeft - oldTime;
}

setInterval(() => {
    if (timerState.isRunning && !timerState.isVictory && !timerState.isRollingBonus) {
        if (timerState.timeLeft > 0) timerState.timeLeft -= 1;

        if (timerState.isFrozen && timerState.freezeTimeLeft > 0) {
            timerState.freezeTimeLeft -= 1;
            if (timerState.freezeTimeLeft <= 0) timerState.isFrozen = false;
        }
        
        if (timerState.timeLeft <= 0) {
            if (timerState.forceVictory) {
                timerState.timeLeft = 0; timerState.isRunning = false; timerState.isVictory = true; timerState.forceVictory = false;
            } else if (timerState.settings.bonusEnabled && !timerState.isBonusPhase) {
                timerState.timeLeft = 0; timerState.isBonusPhase = true; timerState.isRollingBonus = true; timerState.isRunning = false;
                const min = timerState.settings.bonusMin || 60; const max = timerState.settings.bonusMax || 300;
                const bonusTime = Math.floor(Math.random() * (max - min + 1)) + min;
                io.emit('start-bonus-roll', { username: timerState.bonusTriggerUser || 'Зритель', amount: bonusTime });
            } else {
                timerState.timeLeft = 0; timerState.isRunning = false; timerState.isVictory = true;
            }
        }

        if (multiplierState.isActive && multiplierState.timeLeft > 0) {
            multiplierState.timeLeft -= 1;
            if (multiplierState.timeLeft <= 0) { multiplierState.isActive = false; multiplierState.value = 1; }
        }
        broadcastTime();
    }
}, 1000);

function spinRoulette(user) {
    if (!timerState.settings.rouletteSlots || timerState.settings.rouletteSlots.length === 0) { isRouletteBusy = false; return; }
    
    let enabledSlots = timerState.settings.rouletteSlots.filter(slot => slot.isEnabled !== false);
    if (enabledSlots.length === 0) enabledSlots = timerState.settings.rouletteSlots;
    let availableSlots = enabledSlots;

    if (timerState.isFrozen) {
        availableSlots = availableSlots.filter(slot => !['multiplier', 'debuff', 'freeze'].includes(slot.type));
        if (availableSlots.length === 0) availableSlots = enabledSlots;
    } else {
        availableSlots = availableSlots.filter(slot => slot.type !== 'unfreeze');
        if (availableSlots.length === 0) availableSlots = enabledSlots;
    }

    if (timerState.timeLeft <= 300) {
        availableSlots = availableSlots.filter(slot => !['sub_time', 'divide_time'].includes(slot.type));
        if (availableSlots.length === 0) availableSlots = enabledSlots;
    }
    
    if (timerState.timeLeft < 420) {
        availableSlots = availableSlots.filter(slot => slot.type !== 'debuff');
        if (availableSlots.length === 0) availableSlots = enabledSlots;
    }

    if (multiplierState.isActive) {
        availableSlots = availableSlots.filter(slot => !['multiplier', 'debuff'].includes(slot.type));
        if (availableSlots.length === 0) availableSlots = enabledSlots; 
    }

    let totalWeight = availableSlots.reduce((sum, slot) => sum + Number(slot.chance), 0) || 1;
    let random = Math.random() * totalWeight; let winner = availableSlots[availableSlots.length - 1];
    for (let slot of availableSlots) { if (random < slot.chance) { winner = slot; break; } random -= slot.chance; }

    io.emit('start-roulette', { winner, slots: availableSlots, user });
}

function checkRouletteQueue() {
    if (isRouletteBusy || rouletteQueue.length === 0 || timerState.isVictory) return;
    isRouletteBusy = true; spinRoulette(rouletteQueue.shift());
}

function processGenericDonation(id, username, amount, currency, platform) {
    if (timerState.isVictory || timerState.isRollingBonus) return;
    const rawAmount = parseFloat(amount); if (isNaN(rawAmount) || rawAmount <= 0) return;

    const exchangeRates = { 'USD': 92, 'EUR': 100, 'KZT': 0.2, 'BYN': 28, 'UAH': 2.4, 'RUB': 1 };
    let amountInRub = rawAmount * (exchangeRates[String(currency || 'RUB').toUpperCase()] || 1);

    if (timerState.isFrozen) {
        broadcastAlert({ id: Date.now(), username: username, avatar: 'https://cdn-icons-png.flaticon.com/512/5272/5272370.png', giftName: `Донат ${rawAmount}`, timeAdded: 0, type: 'frozen_gift', amount: 1, targetTime: timerState.timeLeft });
        return;
    }

    let addedTime = addTime(Math.floor(amountInRub), username);
    broadcastAlert({ id: Date.now(), username: username, avatar: 'https://cdn-icons-png.flaticon.com/512/5272/5272370.png', giftName: `Донат ${rawAmount}`, timeAdded: addedTime, type: 'gift', amount: 1, targetTime: timerState.timeLeft });
    broadcastTime();
}

async function establishDaConnection(accessToken) {
    try {
        const res = await fetch('https://www.donationalerts.com/api/v1/user/oauth', { headers: { 'Authorization': `Bearer ${accessToken}` } });
        if (!res.ok) { statusText.da = 'Ошибка токена DA'; broadcastStatus(); return; }
        const userData = await res.json();
        const userId = userData.data.id; const socketToken = userData.data.socket_connection_token;

        daWs = new WebSocket('wss://centrifugo.donationalerts.com/connection/websocket');
        daWs.on('open', () => {
            daWs.send(JSON.stringify({ "params": { "token": socketToken }, "id": 1 }));
            statusText.da = 'Успешно подключено'; broadcastStatus();
            io.emit('play-success-sound');
        });

        daWs.on('message', async (data) => {
            const msg = JSON.parse(data);
            if (msg.id === 1 && msg.result && msg.result.client) {
                try {
                    const subRes = await fetch('https://www.donationalerts.com/api/v1/centrifuge/subscribe', {
                        method: 'POST', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify({ channels: [`$alerts:donation_${userId}`], client: msg.result.client })
                    });
                    const subData = await subRes.json();
                    daWs.send(JSON.stringify({ "params": { "channel": `$alerts:donation_${userId}`, "token": subData.channels[0].token }, "method": 1, "id": 2 }));
                } catch (err) {}
            }
            let result = msg.result || msg;
            if (result && result.channel === `$alerts:donation_${userId}` && result.data && result.data.data) {
                try {
                    let don = typeof result.data.data === 'string' ? JSON.parse(result.data.data) : result.data.data;
                    if (don.amount) processGenericDonation(don.id, don.username, don.amount, don.currency, 'DA');
                } catch (e) {}
            }
        });
        daWs.on('close', () => { statusText.da = 'Соединение разорвано'; broadcastStatus(); });
    } catch (err) { statusText.da = `Ошибка: ${err.message}`; broadcastStatus(); }
}

async function connectDonatePay(apiKey) {
    if (!apiKey) return;
    if (dpInterval) clearInterval(dpInterval);
    statusText.dp = 'Подключение...'; broadcastStatus();
    try {
        const res = await fetch('https://donatepay.ru/api/v1/transactions', {
            method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ access_token: apiKey, limit: '1', type: 'donation', status: 'success' })
        });
        const data = await res.json();
        if (data.status === 'success') {
            statusText.dp = 'Успешно подключено'; broadcastStatus();
            if (data.data && data.data.length > 0) lastDpDonationId = data.data[0].id;
            dpInterval = setInterval(async () => {
                try {
                    const r = await fetch('https://donatepay.ru/api/v1/transactions', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ access_token: apiKey, limit: '10', type: 'donation', status: 'success' }) });
                    const d = await r.json();
                    if (d.status === 'success' && d.data) {
                        d.data.reverse().forEach(don => {
                            if (!lastDpDonationId || don.id > lastDpDonationId) {
                                lastDpDonationId = don.id; processGenericDonation(don.id, don.what || don.name, don.sum, don.currency, 'DP');
                            }
                        });
                    }
                } catch(e) {}
            }, 10000);
            io.emit('play-success-sound');
        } else throw new Error(data.message || 'Ошибка DP');
    } catch (e) { statusText.dp = `Ошибка: ${e.message}`; broadcastStatus(); }
}

async function connectDonateX(token) {
    if (!token) return;
    if (dxInterval) clearInterval(dxInterval);
    statusText.dx = 'Подключение...'; broadcastStatus();
    try {
        const res = await fetch(`https://donatex.gg/api/v1/donations?skip=0&take=1`, { headers: { 'Authorization': `Bearer ${token}` } });
        if (res.ok) {
            statusText.dx = 'Успешно подключено'; broadcastStatus();
            dxInterval = setInterval(async () => {
                try {
                    const r = await fetch(`https://donatex.gg/api/v1/donations?skip=0&take=10`, { headers: { 'Authorization': `Bearer ${token}` } });
                    if (r.ok) {
                        const data = await r.json();
                        if (Array.isArray(data)) data.reverse().forEach(don => {
                            if (!dxProcessedIds.has(don.id)) {
                                dxProcessedIds.add(don.id);
                                if(dxProcessedIds.size > 1000) dxProcessedIds = new Set(Array.from(dxProcessedIds).slice(-100));
                                processGenericDonation(don.id, don.username, don.amountInRub || don.amount, 'RUB', 'DX');
                            }
                        });
                    }
                } catch(e) {}
            }, 10000);
            io.emit('play-success-sound');
        } else throw new Error('Ошибка токена DX');
    } catch (e) { statusText.dx = `Ошибка: ${e.message}`; broadcastStatus(); }
}

function disconnectTikTok() {
    if (tiktokConnection) { 
        try { tiktokConnection.terminate(); /* Используем жесткое завершение вместо мягкого close */ } catch(e) {} 
        tiktokConnection = null; 
    }
    if (tikToolWatchdog) { clearTimeout(tikToolWatchdog); tikToolWatchdog = null; }
    if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }
    if (ttPingInterval) { clearInterval(ttPingInterval); ttPingInterval = null; } // Очищаем пинг
    statusText.tt = { text: 'Отключено', isActive: false }; broadcastStatus();
}

async function connectTikTok(username, apiKey) {
    if (!username || !apiKey) return;
    disconnectTikTok();
    statusText.tt = { text: 'Подключение...', isActive: false }; broadcastStatus();

    try {
        const wsUrl = `wss://api.tik.tools?uniqueId=${encodeURIComponent(username)}&apiKey=${encodeURIComponent(apiKey.trim())}`;
        tiktokConnection = new WebSocket(wsUrl);
        tiktokConnection.isAlive = true; // Флаг активности соединения

        tiktokConnection.on('open', () => {
            ttReconnectAttempts = 0;
            statusText.tt = { text: 'Успешно подключено', isActive: true }; broadcastStatus();
            io.emit('play-success-sound');
            
            // HEARTBEAT МЕХАНИЗМ: Пингуем сервер TikTok каждые 30 секунд
            ttPingInterval = setInterval(() => {
                if (tiktokConnection && tiktokConnection.readyState === WebSocket.OPEN) {
                    if (tiktokConnection.isAlive === false) {
                        // Если ответа на прошлый пинг не было — соединение зависло (Zombied)
                        console.log('[TikTok] Соединение зависло. Принудительное переподключение...');
                        return tiktokConnection.terminate();
                    }
                    tiktokConnection.isAlive = false;
                    tiktokConnection.ping(); // Отправляем ping
                }
            }, 30000);

            tikToolWatchdog = setTimeout(disconnectTikTok, 180000);
        });

        // Слушаем ответный pong от сервера TikTok
        tiktokConnection.on('pong', () => {
            if (tiktokConnection) tiktokConnection.isAlive = true;
        });

        tiktokConnection.on('message', (msg) => {
            if (tiktokConnection) tiktokConnection.isAlive = true; // Любое сообщение означает, что сокет жив
            if (tikToolWatchdog) { clearTimeout(tikToolWatchdog); tikToolWatchdog = setTimeout(disconnectTikTok, 180000); }
            try {
                const events = JSON.parse(msg.toString());
                (Array.isArray(events) ? events : [events]).forEach(evt => {
                    const eventName = evt.event || evt.type || evt.action;
                    const data = evt.data || evt.payload || evt;
                    
                    if (eventName === 'gift') handleTikTokGift(data);
                    else if (eventName === 'like') handleTikTokLike(data);
                    else if (eventName === 'follow') handleTikTokFollow(data);
                });
            } catch (e) {}
        });

        tiktokConnection.on('close', (code) => { 
            if (ttPingInterval) { clearInterval(ttPingInterval); ttPingInterval = null; }
            
            // 4001, 4003 - Ошибки ключа. 4005, 4404 - Стрим реально завершен.
            if (code === 4001 || code === 4003 || code === 4005 || code === 4404) {
                disconnectTikTok();
            } else {
                statusText.tt = { text: `Обрыв (${code}). Переподключение...`, isActive: false }; broadcastStatus();
                ttReconnectAttempts++;
                let delay = ttReconnectAttempts >= 3 ? 120000 : (ttReconnectAttempts === 2 ? 30000 : 10000);
                reconnectTimeout = setTimeout(() => connectTikTok(username, apiKey), delay);
            }
        });
        
        tiktokConnection.on('error', () => {});
    } catch (err) { disconnectTikTok(); }
}

function handleTikTokGift(data) {
    if (timerState.isVictory || timerState.isRollingBonus) return;
    const isEnd = data.repeatEnd !== undefined ? data.repeatEnd : true;
    if (data.giftType === 1 && !isEnd) return;

    const giftIdStr = String(data.giftId || data.gift?.id || '');
    const count = data.repeatCount || data.combo || 1;
    const diamonds = data.diamondCount || data.gift?.diamonds || 0;
    const totalCoins = diamonds * count;
    const nickname = data.nickname || data.user?.nickname || 'Зритель';
    const avatar = data.profilePictureUrl || data.user?.avatarUrl || 'https://via.placeholder.com/48';
    const giftName = data.giftName || data.gift?.name || 'Подарок';
    const giftIcon = data.giftPictureUrl || data.gift?.icon || '';

    io.emit('check-and-save-gift', { gift_id: giftIdStr, name: giftName, icon: giftIcon, cost: diamonds });

    let addedTime = 0; let eventType = 'gift';

    // 1. РАЗМОРОЗКА
    if (timerState.isFrozen && timerState.settings.isFreezeEnabled !== false && checkIds(timerState.settings.giftUnfreezeIds, giftIdStr)) {
        timerState.freezeTimeLeft -= (timerState.settings.unfreezeDuration || 60) * count;
        if (timerState.freezeTimeLeft <= 0) { timerState.isFrozen = false; timerState.freezeTimeLeft = 0; }
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'unfreeze', amount: count, targetTime: timerState.timeLeft });
        broadcastTime(); return;
    }

    // 2. РУЛЕТКА
    if (checkIds(timerState.settings.giftRouletteIds, giftIdStr)) {
        for(let i=0; i<count; i++) rouletteQueue.push({ username: nickname, avatar, triggerGift: { name: giftName, icon: giftIcon } });
        checkRouletteQueue();
        if (timerState.settings.rouletteGiftAddsCost && totalCoins > 0 && !timerState.isFrozen) {
            addedTime = addTime(totalCoins, nickname, true);
        }
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: addedTime, type: 'roulette', amount: count, targetTime: timerState.timeLeft });
        broadcastTime(); return;
    }

    // 3. ЗАМОРОЗКА
    if (timerState.settings.isFreezeEnabled !== false && checkIds(timerState.settings.giftFreezeIds, giftIdStr)) {
        timerState.isFrozen = true; timerState.freezeTimeLeft += (timerState.settings.freezeDuration || 60) * count;
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'freeze', amount: count, targetTime: timerState.timeLeft });
        broadcastTime(); return;
    }

    // Если заморожен - блокируем все остальное
    if (timerState.isFrozen) {
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'frozen_gift', amount: count, targetTime: timerState.timeLeft });
        return;
    }

    // Баффы / Дебаффы
    if (timerState.settings.isMultiplierGiftEnabled && checkIds(timerState.settings.giftMultiplierIds, giftIdStr)) {
        multiplierState = { isActive: true, type: 'buff', value: timerState.settings.multiplierValue || 2, timeLeft: (timerState.settings.multiplierDuration || 60) * count };
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'multiplier', amount: count, targetTime: timerState.timeLeft });
        broadcastTime(); return;
    }

    if (timerState.settings.isDebuffGiftEnabled && checkIds(timerState.settings.giftDebuffIds, giftIdStr)) {
        multiplierState = { isActive: true, type: 'debuff', value: timerState.settings.debuffValue || 2, timeLeft: (timerState.settings.debuffDuration || 60) * count };
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'debuff', amount: count, targetTime: timerState.timeLeft });
        broadcastTime(); return;
    }

    // Штрафы, Обнуления
    if (timerState.settings.isPenaltyEnabled !== false && checkIds(timerState.settings.giftPenaltyIds, giftIdStr)) {
        eventType = 'penalty';
        let mult = multiplierState.isActive ? multiplierState.value : 1; 
        let basePenalty = (timerState.settings.penaltyAmount || 600); 
        let threshold = (timerState.settings.penaltyThreshold || 300); 
        let timeToSubtract = basePenalty * mult; 
        for (let i = 0; i < count; i++) {
            if (timerState.timeLeft > threshold) {
                let diff = timerState.timeLeft - timeToSubtract; 
                if (diff < threshold) diff = threshold; 
                addedTime -= (timerState.timeLeft - diff); timerState.timeLeft = diff;
            }
        }
    } else if (timerState.settings.isSetTimeEnabled !== false && checkIds(timerState.settings.giftSetTimeIds, giftIdStr)) {
        eventType = 'set_time';
        let targetTime = timerState.settings.setTimeValue || 300; 
        addedTime = targetTime - timerState.timeLeft; timerState.timeLeft = targetTime;
        if (timerState.timeLeft > 0) { timerState.isVictory = false; if (!timerState.isBonusPhase && !timerState.isRollingBonus) timerState.isRunning = true; }
    } else if (timerState.settings.isResetEnabled !== false && checkIds(timerState.settings.giftResetIds, giftIdStr)) {
        eventType = 'reset_time';
        addedTime = -timerState.timeLeft; timerState.timeLeft = 0; timerState.forceVictory = true; 
    } else {
        // Кастомные правила
        const customRule = timerState.settings.customTriggers?.find(rule => rule.isEnabled && checkIds(rule.ids, giftIdStr));
        if (customRule) {
            eventType = customRule.type;
            if (customRule.type === 'add') addedTime = addTime((customRule.value || 0) * count, nickname);
            else if (customRule.type === 'sub') addedTime = addTime(-Math.abs(customRule.value || 0) * count, nickname);
            else if (customRule.type === 'set') { 
                let targetTime = customRule.value || 0; addedTime = targetTime - timerState.timeLeft; timerState.timeLeft = targetTime; 
                if (timerState.timeLeft > 0) { timerState.isVictory = false; if (!timerState.isBonusPhase && !timerState.isRollingBonus) timerState.isRunning = true; }
            }
            else if (customRule.type === 'reset') { addedTime = -timerState.timeLeft; timerState.timeLeft = 0; timerState.forceVictory = true; }
        } else {
            addedTime = addTime(totalCoins, nickname);
        }
    }

    if (addedTime !== 0 || totalCoins > 0 || ['set_time', 'reset_time', 'set', 'reset', 'penalty'].includes(eventType)) { 
        broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: addedTime, type: eventType, amount: count, targetTime: timerState.timeLeft }); 
    }
    broadcastTime();
}

function handleTikTokLike(data) {
    if (timerState.isVictory || timerState.isRollingBonus) return;
    const batchLikes = parseInt(data.likeCount || 1, 10);
    const apiTotalLikes = parseInt(data.totalLikes, 10);
    
    if (!isNaN(apiTotalLikes) && apiTotalLikes > currentStreamTotalLikes) currentStreamTotalLikes = apiTotalLikes;
    else currentStreamTotalLikes += batchLikes;
    
    // Рулетка за лайки
    if (timerState.settings.likesRouletteEnabled && currentStreamTotalLikes > 0) {
        const threshold = parseInt(timerState.settings.likesRouletteThreshold) || 100000;
        const currentMilestone = Math.floor(currentStreamTotalLikes / threshold);
        if (lastProcessedLikesMilestone === null) lastProcessedLikesMilestone = currentMilestone;
        else if (currentMilestone > lastProcessedLikesMilestone) {
            for (let i = 0; i < (currentMilestone - lastProcessedLikesMilestone); i++) {
                rouletteQueue.push({ username: 'Лайки', avatar: 'https://cdn-icons-png.flaticon.com/512/833/833472.png', triggerGift: { name: 'Лайки', icon: 'https://cdn-icons-png.flaticon.com/512/833/833472.png' }, isLikesRoulette: true });
            }
            lastProcessedLikesMilestone = currentMilestone; checkRouletteQueue();
        }
    }

    if (!timerState.settings.likesEnabled) return;
    const limit = parseInt(timerState.settings.likeThreshold) || 100;
    const userId = data.uniqueId || String(Math.random());
    timerState.userLikes[userId] = (timerState.userLikes[userId] || 0) + batchLikes;

    let triggers = Math.floor(timerState.userLikes[userId] / limit);
    if (triggers > 0) {
        timerState.userLikes[userId] -= triggers * limit;
        for (let i = 0; i < triggers; i++) {
            if (timerState.isFrozen) {
                broadcastAlert({ id: Date.now()+i, username: data.nickname, avatar: data.profilePictureUrl, giftName: "Лайки", timeAdded: 0, type: 'frozen_gift', amount: limit, targetTime: timerState.timeLeft });
            } else {
                let addedTime = addTime(timerState.settings.likeTime, data.nickname, true);
                broadcastAlert({ id: Date.now()+i, username: data.nickname, avatar: data.profilePictureUrl, timeAdded: addedTime, type: 'like', amount: limit, targetTime: timerState.timeLeft });
            }
        }
    }
    broadcastTime();
}

function handleTikTokFollow(data) {
    if (timerState.isVictory || timerState.isRollingBonus || !timerState.settings.subsEnabled) return;
    const userId = data.uniqueId || String(Math.random());
    if (!timerState.subbedUsers.has(userId)) {
        timerState.subbedUsers.add(userId);
        if (timerState.isFrozen) {
            broadcastAlert({ id: Date.now(), username: data.nickname, avatar: data.profilePictureUrl, giftName: "Подписка", timeAdded: 0, type: 'frozen_gift', targetTime: timerState.timeLeft });
        } else {
            let addedTime = addTime(timerState.settings.subTime, data.nickname, true);
            broadcastAlert({ id: Date.now(), username: data.nickname, avatar: data.profilePictureUrl, timeAdded: addedTime, type: 'follow', targetTime: timerState.timeLeft });
        }
        broadcastTime();
    }
}

io.on('connection', (socket) => {
    socket.emit('status-update', statusText);
    socket.emit('settings-updated', timerState.settings);
    socket.emit('timer-tick', { 
        timeLeft: timerState.timeLeft, isVictory: timerState.isVictory, isRunning: timerState.isRunning,
        multiplier: multiplierState, isFrozen: timerState.isFrozen, freezeTimeLeft: timerState.freezeTimeLeft,
        currentTotalLikes: currentStreamTotalLikes, likesRouletteThreshold: timerState.settings?.likesRouletteThreshold || 100000,
        likesRouletteEnabled: timerState.settings?.likesRouletteEnabled || false
    });

    socket.on('update-settings', (config) => {
        timerState.settings = config;
        timerState.settings.originalRouletteSlots = [...(config.rouletteSlots || [])];
        io.emit('settings-updated', config);
    });

    socket.on('start-timer', (config) => {
        timerState.settings = config;
        timerState.settings.originalRouletteSlots = [...(config.rouletteSlots || [])];
        timerState.timeLeft = config.initialTime * 60;
        timerState.isRunning = true; timerState.isVictory = false; timerState.isBonusPhase = false;
        multiplierState.isActive = false; rouletteQueue = []; isRouletteBusy = false;
        timerState.isFrozen = false; timerState.freezeTimeLeft = 0;
        timerState.subbedUsers.clear(); timerState.userLikes = {};
        lastProcessedLikesMilestone = null;
        broadcastTime();
    });

    socket.on('stop-timer', () => { timerState.isRunning = false; broadcastTime(); });
    socket.on('toggle-timer', () => { timerState.isRunning = !timerState.isRunning; broadcastTime(); });
    
    socket.on('manual-add', (sec) => { timerState.timeLeft += sec; if(timerState.timeLeft > 0) timerState.isVictory = false; broadcastTime(); });
    socket.on('manual-sub', (sec) => { timerState.timeLeft = Math.max(0, timerState.timeLeft - sec); if(timerState.timeLeft === 0) { timerState.isVictory = true; timerState.isRunning = false; } broadcastTime(); });
    socket.on('manual-freeze', () => { timerState.isFrozen = true; timerState.freezeTimeLeft += timerState.settings.freezeDuration || 60; broadcastTime(); });
    socket.on('manual-unfreeze', () => { timerState.freezeTimeLeft -= timerState.settings.unfreezeDuration || 60; if(timerState.freezeTimeLeft <= 0) {timerState.isFrozen = false; timerState.freezeTimeLeft = 0;} broadcastTime(); });
    
    socket.on('manual-trigger-roulette', () => {
        rouletteQueue.push({ username: 'Стример', avatar: 'https://cdn-icons-png.flaticon.com/512/2888/2888661.png', triggerGift: { name: 'Пульт', icon: '' } });
        checkRouletteQueue();
    });

    socket.on('apply-roulette-result', (payload) => {
        const { winner, user } = payload;
        let addedTime = 0, eventType = 'roulette', alertText = winner.label, isSpecial = false;

        if (winner.isEliminable) {
            let updatedSlots = timerState.settings.rouletteSlots.filter(s => s.id !== winner.id);
            if (updatedSlots.length === 0) updatedSlots = [...(timerState.settings.originalRouletteSlots || [])];
            timerState.settings.rouletteSlots = updatedSlots;
            io.emit('settings-updated', timerState.settings);
        }

        if (winner.type === 'set_time') { timerState.timeLeft = winner.value; addedTime = winner.value; }
        else if (winner.type === 'reset_time') { timerState.timeLeft = 0; timerState.forceVictory = true; addedTime = 0; }
        else if (winner.type === 'freeze') { timerState.isFrozen = true; timerState.freezeTimeLeft += winner.value || 60; alertText = '❄️ ЗАМОРОЗКА!'; isSpecial = true; }
        else if (winner.type === 'unfreeze') { timerState.freezeTimeLeft -= timerState.settings.unfreezeDuration || 60; if(timerState.freezeTimeLeft <= 0) {timerState.isFrozen = false; timerState.freezeTimeLeft = 0;} alertText = '🔥 РАЗМОРОЗКА!'; isSpecial = true; }
        else if (winner.type === 'extra_spin') {
            alertText = `ЕЩЕ ПРОКРУТ (x${winner.value||1})`; isSpecial = true;
            for (let i = 0; i < (winner.value||1); i++) rouletteQueue.unshift({ username: user.username, avatar: user.avatar, triggerGift: { name: 'Доп. прокрут', icon: 'https://cdn-icons-png.flaticon.com/512/808/808271.png' } });
        }
        else if (winner.type === 'multiplier' || winner.type === 'debuff') {
            multiplierState = { isActive: true, type: winner.type === 'debuff' ? 'debuff' : 'buff', value: winner.multiplierValue || 2, timeLeft: winner.value || 60 };
        }
        else if (winner.type === 'multiply_time') { let old = timerState.timeLeft; timerState.timeLeft = Math.floor(old * (winner.multiplierValue||2)); addedTime = timerState.timeLeft - old; alertText = `Время x${winner.multiplierValue}`; }
        else if (winner.type === 'divide_time') { let old = timerState.timeLeft; let nTime = Math.floor(old / Math.max(1, (winner.multiplierValue||2))); if (old > 300 && nTime < 300) nTime = 300; timerState.timeLeft = nTime; addedTime = nTime - old; alertText = `Время /${winner.multiplierValue}`; }
        else if (winner.type === 'special') { isSpecial = true; alertText = winner.textValue || winner.label; }
        else if (winner.type === 'add_time') { timerState.timeLeft += winner.value; addedTime = winner.value; }
        else if (winner.type === 'sub_time') { let old = timerState.timeLeft; let nTime = old - Math.abs(winner.value); if (old > 300 && nTime < 300) nTime = 300; timerState.timeLeft = nTime; addedTime = nTime - old; }
        
        broadcastAlert({ id: Date.now(), username: user.username, avatar: user.avatar, giftName: alertText, timeAdded: addedTime, type: eventType, isSpecial, targetTime: timerState.timeLeft });
        broadcastTime();
    });

    socket.on('roulette-animation-finished', () => { isRouletteBusy = false; setTimeout(checkRouletteQueue, 1000); });
    socket.on('bonus-roll-finished', (time) => { timerState.timeLeft = time; timerState.isRollingBonus = false; timerState.isRunning = true; broadcastTime(); });

    socket.on('connect-tiktok', (d) => connectTikTok(d.username, d.apiKey)); socket.on('disconnect-tiktok', disconnectTikTok);
    socket.on('connect-da-token', (token) => establishDaConnection(token)); socket.on('disconnect-da', () => { if(daWs) daWs.close(); daWs=null; statusText.da = 'Отключено'; broadcastStatus(); });
    socket.on('connect-dp', (apiKey) => connectDonatePay(apiKey)); socket.on('disconnect-dp', () => { clearInterval(dpInterval); dpInterval=null; statusText.dp = 'Отключено'; broadcastStatus(); });
    socket.on('connect-dx', (token) => connectDonateX(token)); socket.on('disconnect-dx', () => { clearInterval(dxInterval); dxInterval=null; statusText.dx = 'Отключено'; broadcastStatus(); });
});

// Слушаем порт (Railway подставляет процесс.енв.ПОРТ)
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
