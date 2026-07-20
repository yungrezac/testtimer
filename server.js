const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, '/')));

app.get('/da-callback', (req, res) => {
    res.send(`<html><body><script>
        if (window.opener) { window.opener.postMessage({ type: 'da_auth', hash: window.location.hash }, '*'); window.close(); } 
        else { document.write('Авторизация успешна. Можете закрыть окно.'); }
    </script></body></html>`);
});

class UserSession {
    constructor(userId, ioServer) {
        this.userId = userId;
        this.io = ioServer;
        
        this.timerState = {
            timeLeft: 0, isRunning: false, isVictory: false, isBonusPhase: false, isRollingBonus: false,
            bonusTriggerUser: '', forceVictory: false, isFrozen: false, freezeTimeLeft: 0,
            settings: {}, subbedUsers: new Set(), userLikes: {}
        };
        this.multiplierState = { isActive: false, type: 'buff', value: 1, timeLeft: 0 };
        
        this.rouletteQueue = [];
        this.isRouletteBusy = false;
        
        // Подключения TikTok
        this.tiktokConnection = null;
        this.tikToolWatchdog = null;
        this.ttPingInterval = null;
        this.reconnectTimeout = null;
        this.ttReconnectAttempts = 0;
        this.currentStreamTotalLikes = 0;
        this.lastProcessedLikesMilestone = null;
        
        // Подключения DA, DP, DX
        this.daWs = null;
        this.dpInterval = null;
        this.dxInterval = null;
        this.lastDpDonationId = null;
        this.dxProcessedIds = new Set();
        
        this.statusText = { tt: { text: 'Ожидание', isActive: false }, da: 'Ожидание', dp: 'Ожидание', dx: 'Ожидание' };
        
        this.startEngine();
    }

    emit(event, data) { this.io.to(this.userId).emit(event, data); }

    startEngine() {
        this.mainInterval = setInterval(() => {
            if (this.timerState.isRunning && !this.timerState.isVictory && !this.timerState.isRollingBonus) {
                if (this.timerState.timeLeft > 0) this.timerState.timeLeft -= 1;

                if (this.timerState.isFrozen && this.timerState.freezeTimeLeft > 0) {
                    this.timerState.freezeTimeLeft -= 1;
                    if (this.timerState.freezeTimeLeft <= 0) this.timerState.isFrozen = false;
                }
                
                if (this.timerState.timeLeft <= 0) {
                    if (this.timerState.forceVictory) {
                        this.timerState.timeLeft = 0; this.timerState.isRunning = false; this.timerState.isVictory = true; this.timerState.forceVictory = false;
                    } else if (this.timerState.settings.bonusEnabled && !this.timerState.isBonusPhase) {
                        this.timerState.timeLeft = 0; this.timerState.isBonusPhase = true; this.timerState.isRollingBonus = true; this.timerState.isRunning = false;
                        const min = this.timerState.settings.bonusMin || 60; const max = this.timerState.settings.bonusMax || 300;
                        const bonusTime = Math.floor(Math.random() * (max - min + 1)) + min;
                        this.emit('start-bonus-roll', { username: this.timerState.bonusTriggerUser || 'Зритель', amount: bonusTime });
                    } else {
                        this.timerState.timeLeft = 0; this.timerState.isRunning = false; this.timerState.isVictory = true;
                    }
                }

                if (this.multiplierState.isActive && this.multiplierState.timeLeft > 0) {
                    this.multiplierState.timeLeft -= 1;
                    if (this.multiplierState.timeLeft <= 0) { this.multiplierState.isActive = false; this.multiplierState.value = 1; }
                }
                this.broadcastTime();
            }
        }, 1000);
    }

    broadcastTime() {
        this.emit('timer-tick', {
            timeLeft: this.timerState.timeLeft, isVictory: this.timerState.isVictory, isRunning: this.timerState.isRunning,
            multiplier: this.multiplierState, isBonusPhase: this.timerState.isBonusPhase, isRollingBonus: this.timerState.isRollingBonus,
            isFrozen: this.timerState.isFrozen, freezeTimeLeft: this.timerState.freezeTimeLeft,
            currentTotalLikes: this.currentStreamTotalLikes,
            likesRouletteThreshold: this.timerState.settings?.likesRouletteThreshold || 100000,
            likesRouletteEnabled: this.timerState.settings?.likesRouletteEnabled || false
        });
    }

    broadcastAlert(data) { this.emit('new-alert', data); }
    broadcastStatus() { this.emit('status-update', this.statusText); }

    addTime(amount, username, ignoreMultiplier = false) {
        if (this.timerState.isVictory) return 0;
        if (this.timerState.isFrozen && !ignoreMultiplier) return 0;

        let timeChange = amount;
        if (this.multiplierState.isActive && !ignoreMultiplier) {
            if (this.multiplierState.type === 'buff') timeChange = amount > 0 ? amount * this.multiplierState.value : amount;
            else if (this.multiplierState.type === 'debuff') timeChange = -Math.abs(amount * this.multiplierState.value);
        }
        
        let oldTime = this.timerState.timeLeft;
        this.timerState.timeLeft += timeChange;
        if (this.timerState.timeLeft <= 0) {
            this.timerState.timeLeft = 0;
            if (oldTime > 0) this.timerState.bonusTriggerUser = username;
        }
        return this.timerState.timeLeft - oldTime;
    }

    checkIds(idsArray, giftId) { return Array.isArray(idsArray) && idsArray.some(id => String(id) === String(giftId)); }

    spinRoulette(user) {
        if (!this.timerState.settings.rouletteSlots || this.timerState.settings.rouletteSlots.length === 0) { this.isRouletteBusy = false; return; }
        
        let enabledSlots = this.timerState.settings.rouletteSlots.filter(s => s.isEnabled !== false);
        if (enabledSlots.length === 0) enabledSlots = this.timerState.settings.rouletteSlots;
        let availableSlots = enabledSlots;

        if (this.timerState.isFrozen) availableSlots = availableSlots.filter(s => !['multiplier', 'debuff', 'freeze'].includes(s.type));
        else availableSlots = availableSlots.filter(s => s.type !== 'unfreeze');
        if (availableSlots.length === 0) availableSlots = enabledSlots;
        
        if (this.timerState.timeLeft <= 300) availableSlots = availableSlots.filter(s => !['sub_time', 'divide_time'].includes(s.type));
        if (availableSlots.length === 0) availableSlots = enabledSlots;

        let totalWeight = availableSlots.reduce((sum, slot) => sum + Number(slot.chance), 0) || 1;
        let random = Math.random() * totalWeight; let winner = availableSlots[availableSlots.length - 1];
        for (let slot of availableSlots) { if (random < slot.chance) { winner = slot; break; } random -= slot.chance; }

        this.emit('start-roulette', { winner, slots: availableSlots, user });
    }

    checkRouletteQueue() {
        if (this.isRouletteBusy || this.rouletteQueue.length === 0 || this.timerState.isVictory) return;
        this.isRouletteBusy = true; this.spinRoulette(this.rouletteQueue.shift());
    }

    processGenericDonation(id, username, amount, currency, platform) {
        if (this.timerState.isVictory || this.timerState.isRollingBonus) return;
        const rawAmount = parseFloat(amount); if (isNaN(rawAmount) || rawAmount <= 0) return;

        const exchangeRates = { 'USD': 92, 'EUR': 100, 'KZT': 0.2, 'BYN': 28, 'UAH': 2.4, 'RUB': 1 };
        let amountInRub = rawAmount * (exchangeRates[String(currency || 'RUB').toUpperCase()] || 1);

        if (this.timerState.isFrozen) {
            this.broadcastAlert({ id: Date.now(), username, avatar: 'https://cdn-icons-png.flaticon.com/512/5272/5272370.png', giftName: `Донат ${rawAmount}`, timeAdded: 0, type: 'frozen_gift', amount: 1, targetTime: this.timerState.timeLeft });
            return;
        }

        let addedTime = this.addTime(Math.floor(amountInRub), username);
        this.broadcastAlert({ id: Date.now(), username, avatar: 'https://cdn-icons-png.flaticon.com/512/5272/5272370.png', giftName: `Донат ${rawAmount}`, timeAdded: addedTime, type: 'gift', amount: 1, targetTime: this.timerState.timeLeft });
        this.broadcastTime();
    }

    disconnectTikTok() {
        if (this.tiktokConnection) { try { this.tiktokConnection.terminate(); } catch(e) {} this.tiktokConnection = null; }
        if (this.tikToolWatchdog) { clearTimeout(this.tikToolWatchdog); this.tikToolWatchdog = null; }
        if (this.reconnectTimeout) { clearTimeout(this.reconnectTimeout); this.reconnectTimeout = null; }
        if (this.ttPingInterval) { clearInterval(this.ttPingInterval); this.ttPingInterval = null; }
        this.statusText.tt = { text: 'Отключено', isActive: false }; this.broadcastStatus();
    }

    connectTikTok(username, apiKey) {
        if (!username || !apiKey) return;
        this.disconnectTikTok();
        this.statusText.tt = { text: 'Подключение...', isActive: false }; this.broadcastStatus();

        try {
            const wsUrl = `wss://api.tik.tools?uniqueId=${encodeURIComponent(username)}&apiKey=${encodeURIComponent(apiKey.trim())}`;
            this.tiktokConnection = new WebSocket(wsUrl);
            this.tiktokConnection.isAlive = true;

            this.tiktokConnection.on('open', () => {
                this.ttReconnectAttempts = 0;
                this.statusText.tt = { text: 'Успешно подключено', isActive: true }; this.broadcastStatus();
                this.emit('play-success-sound', {});
                
                this.ttPingInterval = setInterval(() => {
                    if (this.tiktokConnection && this.tiktokConnection.readyState === WebSocket.OPEN) {
                        if (this.tiktokConnection.isAlive === false) return this.tiktokConnection.terminate();
                        this.tiktokConnection.isAlive = false; this.tiktokConnection.ping();
                    }
                }, 30000);
                this.tikToolWatchdog = setTimeout(() => this.disconnectTikTok(), 180000);
            });

            this.tiktokConnection.on('pong', () => { if (this.tiktokConnection) this.tiktokConnection.isAlive = true; });

            this.tiktokConnection.on('message', (msg) => {
                if (this.tiktokConnection) this.tiktokConnection.isAlive = true;
                if (this.tikToolWatchdog) { clearTimeout(this.tikToolWatchdog); this.tikToolWatchdog = setTimeout(() => this.disconnectTikTok(), 180000); }
                try {
                    const events = JSON.parse(msg.toString());
                    (Array.isArray(events) ? events : [events]).forEach(evt => {
                        const eventName = evt.event || evt.type || evt.action; const data = evt.data || evt.payload || evt;
                        if (eventName === 'gift') this.handleTikTokGift(data);
                        else if (eventName === 'like') this.handleTikTokLike(data);
                        else if (eventName === 'follow') this.handleTikTokFollow(data);
                    });
                } catch (e) {}
            });

            this.tiktokConnection.on('close', (code) => { 
                if (this.ttPingInterval) clearInterval(this.ttPingInterval);
                if (code === 4001 || code === 4003 || code === 4005 || code === 4404) { this.disconnectTikTok(); } 
                else {
                    this.statusText.tt = { text: `Обрыв (${code}). Переподключение...`, isActive: false }; this.broadcastStatus();
                    this.ttReconnectAttempts++;
                    let delay = this.ttReconnectAttempts >= 3 ? 120000 : (this.ttReconnectAttempts === 2 ? 30000 : 10000);
                    this.reconnectTimeout = setTimeout(() => this.connectTikTok(username, apiKey), delay);
                }
            });
            this.tiktokConnection.on('error', () => {});
        } catch (err) { this.disconnectTikTok(); }
    }

    handleTikTokGift(data) {
        if (this.timerState.isVictory || this.timerState.isRollingBonus) return;
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

        this.emit('check-and-save-gift', { gift_id: giftIdStr, name: giftName, icon: giftIcon, cost: diamonds });

        let addedTime = 0; let eventType = 'gift';

        if (this.timerState.isFrozen && this.timerState.settings.isFreezeEnabled !== false && this.checkIds(this.timerState.settings.giftUnfreezeIds, giftIdStr)) {
            this.timerState.freezeTimeLeft -= (this.timerState.settings.unfreezeDuration || 60) * count;
            if (this.timerState.freezeTimeLeft <= 0) { this.timerState.isFrozen = false; this.timerState.freezeTimeLeft = 0; }
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'unfreeze', amount: count, targetTime: this.timerState.timeLeft });
            this.broadcastTime(); return;
        }

        if (this.checkIds(this.timerState.settings.giftRouletteIds, giftIdStr)) {
            for(let i=0; i<count; i++) this.rouletteQueue.push({ username: nickname, avatar, triggerGift: { name: giftName, icon: giftIcon } });
            this.checkRouletteQueue();
            if (this.timerState.settings.rouletteGiftAddsCost && totalCoins > 0 && !this.timerState.isFrozen) addedTime = this.addTime(totalCoins, nickname, true);
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: addedTime, type: 'roulette', amount: count, targetTime: this.timerState.timeLeft });
            this.broadcastTime(); return;
        }

        if (this.timerState.settings.isFreezeEnabled !== false && this.checkIds(this.timerState.settings.giftFreezeIds, giftIdStr)) {
            this.timerState.isFrozen = true; this.timerState.freezeTimeLeft += (this.timerState.settings.freezeDuration || 60) * count;
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'freeze', amount: count, targetTime: this.timerState.timeLeft });
            this.broadcastTime(); return;
        }

        if (this.timerState.isFrozen) {
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'frozen_gift', amount: count, targetTime: this.timerState.timeLeft });
            return;
        }

        if (this.timerState.settings.isMultiplierGiftEnabled && this.checkIds(this.timerState.settings.giftMultiplierIds, giftIdStr)) {
            this.multiplierState = { isActive: true, type: 'buff', value: this.timerState.settings.multiplierValue || 2, timeLeft: (this.timerState.settings.multiplierDuration || 60) * count };
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'multiplier', amount: count, targetTime: this.timerState.timeLeft });
            this.broadcastTime(); return;
        }

        if (this.timerState.settings.isDebuffGiftEnabled && this.checkIds(this.timerState.settings.giftDebuffIds, giftIdStr)) {
            this.multiplierState = { isActive: true, type: 'debuff', value: this.timerState.settings.debuffValue || 2, timeLeft: (this.timerState.settings.debuffDuration || 60) * count };
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: 0, type: 'debuff', amount: count, targetTime: this.timerState.timeLeft });
            this.broadcastTime(); return;
        }

        if (this.timerState.settings.isPenaltyEnabled !== false && this.checkIds(this.timerState.settings.giftPenaltyIds, giftIdStr)) {
            eventType = 'penalty';
            let mult = this.multiplierState.isActive ? this.multiplierState.value : 1; 
            let basePenalty = (this.timerState.settings.penaltyAmount || 600); 
            let threshold = (this.timerState.settings.penaltyThreshold || 300); 
            let timeToSubtract = basePenalty * mult; 
            for (let i = 0; i < count; i++) {
                if (this.timerState.timeLeft > threshold) {
                    let diff = this.timerState.timeLeft - timeToSubtract; 
                    if (diff < threshold) diff = threshold; 
                    addedTime -= (this.timerState.timeLeft - diff); this.timerState.timeLeft = diff;
                }
            }
        } else if (this.timerState.settings.isSetTimeEnabled !== false && this.checkIds(this.timerState.settings.giftSetTimeIds, giftIdStr)) {
            eventType = 'set_time';
            let targetTime = this.timerState.settings.setTimeValue || 300; 
            addedTime = targetTime - this.timerState.timeLeft; this.timerState.timeLeft = targetTime;
            if (this.timerState.timeLeft > 0) { this.timerState.isVictory = false; if (!this.timerState.isBonusPhase && !this.timerState.isRollingBonus) this.timerState.isRunning = true; }
        } else if (this.timerState.settings.isResetEnabled !== false && this.checkIds(this.timerState.settings.giftResetIds, giftIdStr)) {
            eventType = 'reset_time';
            addedTime = -this.timerState.timeLeft; this.timerState.timeLeft = 0; this.timerState.forceVictory = true; 
        } else {
            const customRule = this.timerState.settings.customTriggers?.find(rule => rule.isEnabled && this.checkIds(rule.ids, giftIdStr));
            if (customRule) {
                eventType = customRule.type;
                if (customRule.type === 'add') addedTime = this.addTime((customRule.value || 0) * count, nickname);
                else if (customRule.type === 'sub') addedTime = this.addTime(-Math.abs(customRule.value || 0) * count, nickname);
                else if (customRule.type === 'set') { 
                    let targetTime = customRule.value || 0; addedTime = targetTime - this.timerState.timeLeft; this.timerState.timeLeft = targetTime; 
                    if (this.timerState.timeLeft > 0) { this.timerState.isVictory = false; if (!this.timerState.isBonusPhase && !this.timerState.isRollingBonus) this.timerState.isRunning = true; }
                }
                else if (customRule.type === 'reset') { addedTime = -this.timerState.timeLeft; this.timerState.timeLeft = 0; this.timerState.forceVictory = true; }
            } else {
                addedTime = this.addTime(totalCoins, nickname);
            }
        }

        if (addedTime !== 0 || totalCoins > 0 || ['set_time', 'reset_time', 'set', 'reset', 'penalty'].includes(eventType)) { 
            this.broadcastAlert({ id: Date.now(), username: nickname, avatar, giftName, giftIcon, timeAdded: addedTime, type: eventType, amount: count, targetTime: this.timerState.timeLeft }); 
        }
        this.broadcastTime();
    }

    handleTikTokLike(data) {
        if (this.timerState.isVictory || this.timerState.isRollingBonus) return;
        const batchLikes = parseInt(data.likeCount || 1, 10);
        const apiTotalLikes = parseInt(data.totalLikes, 10);
        
        if (!isNaN(apiTotalLikes) && apiTotalLikes > this.currentStreamTotalLikes) this.currentStreamTotalLikes = apiTotalLikes;
        else this.currentStreamTotalLikes += batchLikes;
        
        if (this.timerState.settings.likesRouletteEnabled && this.currentStreamTotalLikes > 0) {
            const threshold = parseInt(this.timerState.settings.likesRouletteThreshold) || 100000;
            const currentMilestone = Math.floor(this.currentStreamTotalLikes / threshold);
            if (this.lastProcessedLikesMilestone === null) this.lastProcessedLikesMilestone = currentMilestone;
            else if (currentMilestone > this.lastProcessedLikesMilestone) {
                for (let i = 0; i < (currentMilestone - this.lastProcessedLikesMilestone); i++) {
                    this.rouletteQueue.push({ username: 'Лайки', avatar: 'https://cdn-icons-png.flaticon.com/512/833/833472.png', triggerGift: { name: 'Лайки', icon: 'https://cdn-icons-png.flaticon.com/512/833/833472.png' }, isLikesRoulette: true });
                }
                this.lastProcessedLikesMilestone = currentMilestone; this.checkRouletteQueue();
            }
        }

        if (!this.timerState.settings.likesEnabled) return;
        const limit = parseInt(this.timerState.settings.likeThreshold) || 100;
        const userId = data.uniqueId || String(Math.random());
        this.timerState.userLikes[userId] = (this.timerState.userLikes[userId] || 0) + batchLikes;

        let triggers = Math.floor(this.timerState.userLikes[userId] / limit);
        if (triggers > 0) {
            this.timerState.userLikes[userId] -= triggers * limit;
            for (let i = 0; i < triggers; i++) {
                if (this.timerState.isFrozen) {
                    this.broadcastAlert({ id: Date.now()+i, username: data.nickname, avatar: data.profilePictureUrl, giftName: "Лайки", timeAdded: 0, type: 'frozen_gift', amount: limit, targetTime: this.timerState.timeLeft });
                } else {
                    let addedTime = this.addTime(this.timerState.settings.likeTime, data.nickname, true);
                    this.broadcastAlert({ id: Date.now()+i, username: data.nickname, avatar: data.profilePictureUrl, timeAdded: addedTime, type: 'like', amount: limit, targetTime: this.timerState.timeLeft });
                }
            }
        }
        this.broadcastTime();
    }

    handleTikTokFollow(data) {
        if (this.timerState.isVictory || this.timerState.isRollingBonus || !this.timerState.settings.subsEnabled) return;
        const userId = data.uniqueId || String(Math.random());
        if (!this.timerState.subbedUsers.has(userId)) {
            this.timerState.subbedUsers.add(userId);
            if (this.timerState.isFrozen) {
                this.broadcastAlert({ id: Date.now(), username: data.nickname, avatar: data.profilePictureUrl, giftName: "Подписка", timeAdded: 0, type: 'frozen_gift', targetTime: this.timerState.timeLeft });
            } else {
                let addedTime = this.addTime(this.timerState.settings.subTime, data.nickname, true);
                this.broadcastAlert({ id: Date.now(), username: data.nickname, avatar: data.profilePictureUrl, timeAdded: addedTime, type: 'follow', targetTime: this.timerState.timeLeft });
            }
            this.broadcastTime();
        }
    }

    async connectDaToken(accessToken) {
        try {
            const res = await fetch('https://www.donationalerts.com/api/v1/user/oauth', { headers: { 'Authorization': `Bearer ${accessToken}` } });
            if (!res.ok) { this.statusText.da = 'Ошибка токена DA'; this.broadcastStatus(); return; }
            const userData = await res.json();
            const userId = userData.data.id; const socketToken = userData.data.socket_connection_token;

            this.daWs = new WebSocket('wss://centrifugo.donationalerts.com/connection/websocket');
            this.daWs.on('open', () => {
                this.daWs.send(JSON.stringify({ "params": { "token": socketToken }, "id": 1 }));
                this.statusText.da = 'Успешно подключено'; this.broadcastStatus();
                this.emit('play-success-sound', {});
            });

            this.daWs.on('message', async (data) => {
                const msg = JSON.parse(data);
                if (msg.id === 1 && msg.result && msg.result.client) {
                    try {
                        const subRes = await fetch('https://www.donationalerts.com/api/v1/centrifuge/subscribe', {
                            method: 'POST', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ channels: [`$alerts:donation_${userId}`], client: msg.result.client })
                        });
                        const subData = await subRes.json();
                        this.daWs.send(JSON.stringify({ "params": { "channel": `$alerts:donation_${userId}`, "token": subData.channels[0].token }, "method": 1, "id": 2 }));
                    } catch (err) {}
                }
                let result = msg.result || msg;
                if (result && result.channel === `$alerts:donation_${userId}` && result.data && result.data.data) {
                    try {
                        let don = typeof result.data.data === 'string' ? JSON.parse(result.data.data) : result.data.data;
                        if (don.amount) this.processGenericDonation(don.id, don.username, don.amount, don.currency, 'DA');
                    } catch (e) {}
                }
            });
            this.daWs.on('close', () => { this.statusText.da = 'Соединение разорвано'; this.broadcastStatus(); });
        } catch (err) { this.statusText.da = `Ошибка: ${err.message}`; this.broadcastStatus(); }
    }

    disconnectDa() {
        if (this.daWs) { this.daWs.close(); this.daWs = null; }
        this.statusText.da = 'Отключено'; this.broadcastStatus();
    }

    async connectDp(apiKey) {
        if (!apiKey) return;
        if (this.dpInterval) clearInterval(this.dpInterval);
        this.statusText.dp = 'Подключение...'; this.broadcastStatus();
        try {
            const res = await fetch('https://donatepay.ru/api/v1/transactions', {
                method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: new URLSearchParams({ access_token: apiKey, limit: '1', type: 'donation', status: 'success' })
            });
            const data = await res.json();
            if (data.status === 'success') {
                this.statusText.dp = 'Успешно подключено'; this.broadcastStatus();
                if (data.data && data.data.length > 0) this.lastDpDonationId = data.data[0].id;
                this.dpInterval = setInterval(async () => {
                    try {
                        const r = await fetch('https://donatepay.ru/api/v1/transactions', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ access_token: apiKey, limit: '10', type: 'donation', status: 'success' }) });
                        const d = await r.json();
                        if (d.status === 'success' && d.data) {
                            d.data.reverse().forEach(don => {
                                if (!this.lastDpDonationId || don.id > this.lastDpDonationId) {
                                    this.lastDpDonationId = don.id; this.processGenericDonation(don.id, don.what || don.name, don.sum, don.currency, 'DP');
                                }
                            });
                        }
                    } catch(e) {}
                }, 10000);
                this.emit('play-success-sound', {});
            } else throw new Error(data.message || 'Ошибка DP');
        } catch (e) { this.statusText.dp = `Ошибка: ${e.message}`; this.broadcastStatus(); }
    }

    disconnectDp() {
        if (this.dpInterval) { clearInterval(this.dpInterval); this.dpInterval = null; }
        this.statusText.dp = 'Отключено'; this.broadcastStatus();
    }

    async connectDx(token) {
        if (!token) return;
        if (this.dxInterval) clearInterval(this.dxInterval);
        this.statusText.dx = 'Подключение...'; this.broadcastStatus();
        try {
            const res = await fetch(`https://donatex.gg/api/v1/donations?skip=0&take=1`, { headers: { 'Authorization': `Bearer ${token}` } });
            if (res.ok) {
                this.statusText.dx = 'Успешно подключено'; this.broadcastStatus();
                this.dxInterval = setInterval(async () => {
                    try {
                        const r = await fetch(`https://donatex.gg/api/v1/donations?skip=0&take=10`, { headers: { 'Authorization': `Bearer ${token}` } });
                        if (r.ok) {
                            const data = await r.json();
                            if (Array.isArray(data)) data.reverse().forEach(don => {
                                if (!this.dxProcessedIds.has(don.id)) {
                                    this.dxProcessedIds.add(don.id);
                                    if(this.dxProcessedIds.size > 1000) this.dxProcessedIds = new Set(Array.from(this.dxProcessedIds).slice(-100));
                                    this.processGenericDonation(don.id, don.username, don.amountInRub || don.amount, 'RUB', 'DX');
                                }
                            });
                        }
                    } catch(e) {}
                }, 10000);
                this.emit('play-success-sound', {});
            } else throw new Error('Ошибка токена DX');
        } catch (e) { this.statusText.dx = `Ошибка: ${e.message}`; this.broadcastStatus(); }
    }

    disconnectDx() {
        if (this.dxInterval) { clearInterval(this.dxInterval); this.dxInterval = null; }
        this.statusText.dx = 'Отключено'; this.broadcastStatus();
    }
}

const sessions = new Map();

function getSession(userId) {
    if (!sessions.has(userId)) sessions.set(userId, new UserSession(userId, io));
    return sessions.get(userId);
}

io.on('connection', (socket) => {
    let userId = null;

    socket.on('join-room', (uid) => {
        userId = uid;
        socket.join(userId);
        const session = getSession(userId);
        socket.emit('status-update', session.statusText);
        socket.emit('settings-updated', session.timerState.settings);
        session.broadcastTime();
    });

    socket.on('update-settings', (config) => {
        if (!userId) return; const session = getSession(userId);
        session.timerState.settings = config;
        session.timerState.settings.originalRouletteSlots = [...(config.rouletteSlots || [])];
        session.emit('settings-updated', config);
    });

    socket.on('start-timer', (config) => {
        if (!userId) return; const session = getSession(userId);
        session.timerState.settings = config; session.timerState.settings.originalRouletteSlots = [...(config.rouletteSlots || [])];
        session.timerState.timeLeft = config.initialTime * 60; session.timerState.isRunning = true; session.timerState.isVictory = false; session.timerState.isBonusPhase = false;
        session.multiplierState.isActive = false; session.rouletteQueue = []; session.isRouletteBusy = false;
        session.timerState.isFrozen = false; session.timerState.freezeTimeLeft = 0; session.timerState.subbedUsers.clear(); session.timerState.userLikes = {};
        session.lastProcessedLikesMilestone = null;
        session.broadcastTime();
    });

    socket.on('stop-timer', () => { if (!userId) return; const s = getSession(userId); s.timerState.isRunning = false; s.broadcastTime(); });
    socket.on('toggle-timer', () => { if (!userId) return; const s = getSession(userId); s.timerState.isRunning = !s.timerState.isRunning; s.broadcastTime(); });
    socket.on('manual-add', (sec) => { if (!userId) return; const s = getSession(userId); s.timerState.timeLeft += sec; if(s.timerState.timeLeft > 0) s.timerState.isVictory = false; s.broadcastTime(); });
    socket.on('manual-sub', (sec) => { if (!userId) return; const s = getSession(userId); s.timerState.timeLeft = Math.max(0, s.timerState.timeLeft - sec); if(s.timerState.timeLeft === 0) { s.timerState.isVictory = true; s.timerState.isRunning = false; } s.broadcastTime(); });
    socket.on('manual-freeze', () => { if (!userId) return; const s = getSession(userId); s.timerState.isFrozen = true; s.timerState.freezeTimeLeft += s.timerState.settings.freezeDuration || 60; s.broadcastTime(); });
    socket.on('manual-unfreeze', () => { if (!userId) return; const s = getSession(userId); s.timerState.freezeTimeLeft -= s.timerState.settings.unfreezeDuration || 60; if(s.timerState.freezeTimeLeft <= 0) {s.timerState.isFrozen = false; s.timerState.freezeTimeLeft = 0;} s.broadcastTime(); });
    socket.on('manual-trigger-roulette', () => { if (!userId) return; const s = getSession(userId); s.rouletteQueue.push({ username: 'Стример', avatar: 'https://cdn-icons-png.flaticon.com/512/2888/2888661.png', triggerGift: { name: 'Пульт', icon: '' } }); s.checkRouletteQueue(); });

    socket.on('apply-roulette-result', (payload) => {
        if (!userId) return; const session = getSession(userId);
        const { winner, user } = payload;
        let addedTime = 0, eventType = 'roulette', alertText = winner.label, isSpecial = false;

        if (winner.isEliminable) {
            let updatedSlots = session.timerState.settings.rouletteSlots.filter(s => s.id !== winner.id);
            if (updatedSlots.length === 0) updatedSlots = [...(session.timerState.settings.originalRouletteSlots || [])];
            session.timerState.settings.rouletteSlots = updatedSlots; session.emit('settings-updated', session.timerState.settings);
        }

        if (winner.type === 'set_time') { session.timerState.timeLeft = winner.value; addedTime = winner.value; }
        else if (winner.type === 'reset_time') { session.timerState.timeLeft = 0; session.timerState.forceVictory = true; addedTime = 0; }
        else if (winner.type === 'freeze') { session.timerState.isFrozen = true; session.timerState.freezeTimeLeft += winner.value || 60; alertText = '❄️ ЗАМОРОЗКА!'; isSpecial = true; }
        else if (winner.type === 'unfreeze') { session.timerState.freezeTimeLeft -= session.timerState.settings.unfreezeDuration || 60; if(session.timerState.freezeTimeLeft <= 0) {session.timerState.isFrozen = false; session.timerState.freezeTimeLeft = 0;} alertText = '🔥 РАЗМОРОЗКА!'; isSpecial = true; }
        else if (winner.type === 'extra_spin') {
            alertText = `ЕЩЕ ПРОКРУТ (x${winner.value||1})`; isSpecial = true;
            for (let i = 0; i < (winner.value||1); i++) session.rouletteQueue.unshift({ username: user.username, avatar: user.avatar, triggerGift: { name: 'Доп. прокрут', icon: 'https://cdn-icons-png.flaticon.com/512/808/808271.png' } });
        }
        else if (winner.type === 'multiplier' || winner.type === 'debuff') { session.multiplierState = { isActive: true, type: winner.type === 'debuff' ? 'debuff' : 'buff', value: winner.multiplierValue || 2, timeLeft: winner.value || 60 }; }
        else if (winner.type === 'multiply_time') { let old = session.timerState.timeLeft; session.timerState.timeLeft = Math.floor(old * (winner.multiplierValue||2)); addedTime = session.timerState.timeLeft - old; alertText = `Время x${winner.multiplierValue}`; }
        else if (winner.type === 'divide_time') { let old = session.timerState.timeLeft; let nTime = Math.floor(old / Math.max(1, (winner.multiplierValue||2))); if (old > 300 && nTime < 300) nTime = 300; session.timerState.timeLeft = nTime; addedTime = nTime - old; alertText = `Время /${winner.multiplierValue}`; }
        else if (winner.type === 'special') { isSpecial = true; alertText = winner.textValue || winner.label; }
        else if (winner.type === 'add_time') { session.timerState.timeLeft += winner.value; addedTime = winner.value; }
        else if (winner.type === 'sub_time') { let old = session.timerState.timeLeft; let nTime = old - Math.abs(winner.value); if (old > 300 && nTime < 300) nTime = 300; session.timerState.timeLeft = nTime; addedTime = nTime - old; }
        
        session.broadcastAlert({ id: Date.now(), username: user.username, avatar: user.avatar, giftName: alertText, timeAdded: addedTime, type: eventType, isSpecial, targetTime: session.timerState.timeLeft });
        session.broadcastTime();
    });

    socket.on('roulette-animation-finished', () => { if(!userId) return; const s = getSession(userId); s.isRouletteBusy = false; setTimeout(()=>s.checkRouletteQueue(), 1000); });
    socket.on('bonus-roll-finished', (time) => { if(!userId) return; const s = getSession(userId); s.timerState.timeLeft = time; s.timerState.isRollingBonus = false; s.timerState.isRunning = true; s.broadcastTime(); });

    socket.on('connect-tiktok', (d) => { if(userId) getSession(userId).connectTikTok(d.username, d.apiKey); });
    socket.on('disconnect-tiktok', () => { if(userId) getSession(userId).disconnectTikTok(); });
    socket.on('connect-da-token', (token) => { if(userId) getSession(userId).connectDaToken(token); });
    socket.on('disconnect-da', () => { if(userId) getSession(userId).disconnectDa(); });
    socket.on('connect-dp', (apiKey) => { if(userId) getSession(userId).connectDp(apiKey); });
    socket.on('disconnect-dp', () => { if(userId) getSession(userId).disconnectDp(); });
    socket.on('connect-dx', (token) => { if(userId) getSession(userId).connectDx(token); });
    socket.on('disconnect-dx', () => { if(userId) getSession(userId).disconnectDx(); });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
