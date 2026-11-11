"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.streamRecords = void 0;
/**
 * Firebase Functions entrypoint.
 * Each function lives in its own file under ./functions/
 * and is re-exported here for Firebase to discover during deploy.
 */
var streamRecords_js_1 = require("./functions/streamRecords.js");
Object.defineProperty(exports, "streamRecords", { enumerable: true, get: function () { return streamRecords_js_1.streamRecords; } });
//# sourceMappingURL=index.js.map