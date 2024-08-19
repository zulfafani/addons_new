/** @odoo-module **/

export const LocalStoreHelper = {
    save(key, data) {
        try {
            localStorage.setItem(key, JSON.stringify(data));
        } catch (e) {
            console.error("💥 Failed to save data:", key, e);
        }
    },

    load(key) {
        try {
            const raw = localStorage.getItem(key);
            return raw ? JSON.parse(raw) : [];
        } catch (e) {
            console.error("💥 Failed to load data:", key, e);
            return [];
        }
    },

    clear(key) {
        try {
            localStorage.removeItem(key);
        } catch (e) {
            console.error("💥 Failed to clear data:", key, e);
        }
    },
};
