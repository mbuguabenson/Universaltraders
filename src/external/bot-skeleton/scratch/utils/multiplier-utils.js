/**
 * Multiplier utilities stub
 * Created for compatibility with custom toolbox-items.tsx imports.
 * The new bot-skeleton template removed this file; this stub prevents build failures.
 */

/**
 * Returns whether multiplier contracts are available.
 * In the new API, this is determined dynamically from active_symbols data.
 * Defaulting to true ensures multiplier blocks are shown; visibility is controlled at runtime.
 */
export const isMultipliersAvailable = () => true;

export default {
    isMultipliersAvailable,
};
