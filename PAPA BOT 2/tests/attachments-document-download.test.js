const test = require('node:test');
const assert = require('node:assert/strict');

function loadAttachmentsWithAxiosGet(axiosGet) {
    const attachmentsPath = require.resolve('../src/modules/attachments');
    const axiosPath = require.resolve('axios');

    delete require.cache[attachmentsPath];
    delete require.cache[axiosPath];

    const realAxios = require('axios');
    require.cache[axiosPath] = {
        id: axiosPath,
        filename: axiosPath,
        loaded: true,
        exports: {
            ...realAxios,
            get: axiosGet
        }
    };

    return require('../src/modules/attachments');
}

function loadAttachmentsWithMocks({ axiosGet, axiosPost, vkGet, getUserToken }) {
    const attachmentsPath = require.resolve('../src/modules/attachments');
    const axiosPath = require.resolve('axios');
    const configPath = require.resolve('../src/modules/config');
    const vkApiPath = require.resolve('../src/modules/vk-api');

    for (const modulePath of [attachmentsPath, axiosPath, configPath, vkApiPath]) {
        delete require.cache[modulePath];
    }

    const realAxios = require('axios');
    require.cache[axiosPath] = {
        id: axiosPath,
        filename: axiosPath,
        loaded: true,
        exports: {
            ...realAxios,
            get: axiosGet,
            post: axiosPost
        }
    };
    require.cache[configPath] = {
        id: configPath,
        filename: configPath,
        loaded: true,
        exports: {
            getUserToken: getUserToken || (async () => 'user-token'),
            getVkToken: async () => 'group-token',
            getVkGroupId: () => 240175263
        }
    };
    require.cache[vkApiPath] = {
        id: vkApiPath,
        filename: vkApiPath,
        loaded: true,
        exports: { vkGet }
    };

    return require('../src/modules/attachments');
}

test('VK document download retries redirected psv userapi TLS mismatch through vkuseraudio CDN', async () => {
    const calls = [];
    const originalUrl = 'https://vk.com/s/v1/doc/opaque-token';
    const failedCdnUrl = 'https://psv4.userapi.com/s/v1/d/file.pdf?extra=token';
    const fallbackUrl = 'https://psv4.vkuseraudio.net/s/v1/d/file.pdf?extra=token';
    const options = { responseType: 'arraybuffer', timeout: 60000 };
    const attachments = loadAttachmentsWithAxiosGet(async (url, receivedOptions) => {
        calls.push({ url, options: receivedOptions });
        if (url === originalUrl) {
            const error = new Error('Hostname/IP does not match certificate altnames');
            error.code = 'ERR_TLS_CERT_ALTNAME_INVALID';
            error.request = {
                _redirectable: {
                    _currentUrl: failedCdnUrl
                }
            };
            throw error;
        }
        assert.equal(url, fallbackUrl);
        const redirectOptions = {
            protocol: 'https:',
            hostname: 'psv4.userapi.com',
            host: 'psv4.userapi.com',
            servername: 'psv4.userapi.com',
            headers: { host: 'psv4.userapi.com' }
        };
        receivedOptions.beforeRedirect(redirectOptions);
        assert.equal(redirectOptions.hostname, 'psv4.vkuseraudio.net');
        assert.equal(redirectOptions.host, 'psv4.vkuseraudio.net');
        assert.equal(redirectOptions.servername, 'psv4.vkuseraudio.net');
        assert.equal(redirectOptions.headers.host, 'psv4.vkuseraudio.net');
        return { status: 200, data: Buffer.from('document') };
    });

    const response = await attachments.downloadVkDocument(originalUrl, options);

    assert.equal(response.status, 200);
    assert.deepEqual(calls.map(call => call.url), [originalUrl, fallbackUrl]);
    assert.equal(calls[0].options.responseType, options.responseType);
    assert.equal(calls[0].options.timeout, options.timeout);
    assert.equal(typeof calls[0].options.beforeRedirect, 'function');
});

test('VK document download does not bypass TLS or rewrite unrelated hosts', async () => {
    const originalError = new Error('Hostname/IP does not match certificate altnames');
    originalError.code = 'ERR_TLS_CERT_ALTNAME_INVALID';
    const attachments = loadAttachmentsWithAxiosGet(async () => {
        throw originalError;
    });

    await assert.rejects(
        attachments.downloadVkDocument('https://example.com/file.pdf'),
        error => error === originalError
    );
    assert.equal(
        attachments.getVkDocumentCdnFallbackUrl('https://psv4.userapi.com/path?a=1'),
        'https://psv4.vkuseraudio.net/path?a=1'
    );
    assert.equal(attachments.getVkDocumentCdnFallbackUrl('not a URL'), '');
});

test('message document re-upload requests an upload server for the actual recipient peer', async () => {
    const vkCalls = [];
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => ({ data: Buffer.from('document') }),
        axiosPost: async () => ({ data: { file: 'uploaded-file-token' } }),
        vkGet: async (method, params) => {
            vkCalls.push({ method, params });
            if (method === 'docs.getById') {
                return {
                    response: [{
                        owner_id: 27894453,
                        id: 706080866,
                        title: 'consent.pdf',
                        ext: 'pdf',
                        url: 'https://psv4.vkuserdocs.ru/file.pdf'
                    }]
                };
            }
            if (method === 'docs.getMessagesUploadServer') {
                assert.equal(params.peer_id, 787794248);
                return { response: { upload_url: 'https://upload.vk.test/doc' } };
            }
            if (method === 'docs.save') {
                return { response: { doc: { owner_id: -240175263, id: 99 } } };
            }
            throw new Error(`Unexpected VK method: ${method}`);
        }
    });

    const result = await attachments.processAttachmentWithUserToken(
        'doc27894453_706080866',
        '240175263',
        { target: 'message', peerId: 787794248 }
    );

    assert.equal(result, 'doc-240175263_99');
    assert.deepEqual(vkCalls.map(call => call.method), [
        'docs.getById',
        'docs.getMessagesUploadServer',
        'docs.save'
    ]);
});

test('message document re-upload retries a transient VK upload 405 with a fresh upload server', async () => {
    let uploadServerRequests = 0;
    let uploadAttempts = 0;
    let downloadAttempts = 0;
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            downloadAttempts += 1;
            if (downloadAttempts === 2) {
                const error = new Error('self-signed certificate in certificate chain');
                error.code = 'SELF_SIGNED_CERT_IN_CHAIN';
                throw error;
            }
            return { data: Buffer.from('document') };
        },
        axiosPost: async () => {
            uploadAttempts += 1;
            if (uploadAttempts === 1) {
                const error = new Error('Request failed with status code 405');
                error.response = { status: 405 };
                throw error;
            }
            return { data: { file: 'uploaded-file-token' } };
        },
        vkGet: async method => {
            if (method === 'docs.getById') {
                return {
                    response: [{
                        owner_id: 27894453,
                        id: 706080866,
                        title: 'consent.pdf',
                        ext: 'pdf',
                        url: 'https://psv4.vkuserdocs.ru/file.pdf'
                    }]
                };
            }
            if (method === 'docs.getMessagesUploadServer') {
                uploadServerRequests += 1;
                return { response: { upload_url: `https://upload.vk.test/doc/${uploadServerRequests}` } };
            }
            if (method === 'docs.save') {
                return { response: { doc: { owner_id: 787794248, id: 100 } } };
            }
            throw new Error(`Unexpected VK method: ${method}`);
        }
    });

    const result = await attachments.processAttachmentWithUserToken(
        'doc27894453_706080866',
        '240175263',
        { target: 'message', peerId: 787794248, sleep: async () => {} }
    );

    assert.equal(result, 'doc787794248_100');
    assert.equal(uploadAttempts, 2);
    assert.equal(uploadServerRequests, 2);
    assert.equal(downloadAttempts, 3);
});

test('message document re-upload fails the whole delivery after exhausted transient TLS attempts', async () => {
    let downloadAttempts = 0;
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            downloadAttempts += 1;
            const error = new Error('self-signed certificate in certificate chain');
            error.code = 'SELF_SIGNED_CERT_IN_CHAIN';
            throw error;
        },
        axiosPost: async () => {
            throw new Error('upload must not start');
        },
        vkGet: async method => {
            if (method === 'docs.getById') {
                return {
                    response: [{
                        owner_id: 27894453,
                        id: 706080866,
                        title: 'consent.pdf',
                        ext: 'pdf',
                        url: 'https://psv4.vkuserdocs.ru/file.pdf'
                    }]
                };
            }
            throw new Error(`Unexpected VK method: ${method}`);
        }
    });

    await assert.rejects(
        attachments.processAttachmentWithUserToken(
            'doc27894453_706080866',
            '240175263',
            { target: 'message', peerId: 787794248, sleep: async () => {} }
        ),
        error => error?.code === 'VK_DOCUMENT_REUPLOAD_EXHAUSTED'
    );
    assert.equal(downloadAttempts, 5);
});

test('message document re-upload fails before send when the configured user token cannot read the source doc', async () => {
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            throw new Error('download must not start');
        },
        axiosPost: async () => {
            throw new Error('upload must not start');
        },
        vkGet: async method => {
            assert.equal(method, 'docs.getById');
            return {
                error: {
                    error_code: 4,
                    error_msg: 'User authorization failed: invalid access_token'
                }
            };
        }
    });

    await assert.rejects(
        attachments.processAttachmentWithUserToken(
            'doc27894453_706080866',
            '240175263',
            { target: 'message', peerId: 787794248, sleep: async () => {} }
        ),
        error => error?.code === 'VK_DOCUMENT_SOURCE_UNAVAILABLE' && error?.vkErrorCode === 4
    );
});

test('consent documents upload to the community wall for reusable group ownership', async () => {
    const vkCalls = [];
    const tokenCalls = [];
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            throw new Error('download must not start');
        },
        axiosPost: async url => {
            assert.equal(url, 'https://upload.vk.test/wall-doc');
            return { data: { file: 'wall-doc-token' } };
        },
        vkGet: async (method, params) => {
            vkCalls.push({ method, params });
            if (method === 'docs.getWallUploadServer') {
                assert.equal(params.group_id, 240175263);
                assert.equal(params.access_token, 'user-token');
                return { response: { upload_url: 'https://upload.vk.test/wall-doc' } };
            }
            assert.equal(method, 'docs.save');
            assert.equal(params.group_id, 240175263);
            assert.equal(params.access_token, 'user-token');
            return { response: { doc: { owner_id: -240175263, id: 701 } } };
        },
        getUserToken: async (communityId, profileId) => {
            tokenCalls.push({ communityId, profileId });
            return 'user-token';
        }
    });

    const result = await attachments.uploadToVK(
        Buffer.from('document'),
        'consent.pdf',
        'application/pdf',
        'wall',
        '240175263',
        'profile-42'
    );

    assert.equal(result, 'doc-240175263_701');
    assert.deepEqual(vkCalls.map(call => call.method), [
        'docs.getWallUploadServer',
        'docs.save'
    ]);
    assert.deepEqual(tokenCalls, [{ communityId: '240175263', profileId: 'profile-42' }]);
});

test('consent document falls back to a community-owned messages upload when VK denies wall documents', async () => {
    const vkCalls = [];
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            throw new Error('download must not start');
        },
        axiosPost: async url => {
            assert.equal(url, 'https://upload.vk.test/messages-doc');
            return { data: { file: 'messages-doc-token' } };
        },
        vkGet: async (method, params) => {
            vkCalls.push({ method, params });
            if (method === 'docs.getWallUploadServer') {
                return { error: { error_msg: "Access denied: User can't upload docs to this group" } };
            }
            if (method === 'docs.getMessagesUploadServer') {
                assert.equal(params.peer_id, -240175263);
                assert.equal(params.access_token, 'group-token');
                return { response: { upload_url: 'https://upload.vk.test/messages-doc' } };
            }
            assert.equal(method, 'docs.save');
            assert.equal(params.peer_id, -240175263);
            assert.equal(params.access_token, 'group-token');
            return { response: { doc: { owner_id: -240175263, id: 702 } } };
        }
    });

    const result = await attachments.uploadToVK(
        Buffer.from('document'),
        'consent.pdf',
        'application/pdf',
        'wall',
        '240175263',
        'profile-42'
    );

    assert.equal(result, 'doc-240175263_702');
    assert.deepEqual(vkCalls.map(call => call.method), [
        'docs.getWallUploadServer',
        'docs.getMessagesUploadServer',
        'docs.save'
    ]);
});

test('private video upload and later processing preserve its access_key', async () => {
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            throw new Error('video download should not be needed');
        },
        axiosPost: async () => ({ data: { ok: 1 } }),
        vkGet: async (method, params) => {
            if (method === 'video.save') {
                assert.equal(params.privacy_view, 'all');
                assert.equal(params.group_id, 240175263);
                return {
                    response: {
                        upload_url: 'https://upload.vk.test/video',
                        owner_id: 27894453,
                        video_id: 456,
                        access_key: 'private-video-key'
                    }
                };
            }
            throw new Error(`unexpected VK method: ${method}`);
        }
    });

    const uploaded = await attachments.uploadVideoToMessages(
        Buffer.from('video'),
        'clip.mp4',
        'video/mp4',
        '240175263'
    );

    assert.equal(uploaded, 'video27894453_456_private-video-key');
    assert.equal(
        await attachments.processAttachmentWithUserToken(uploaded, '240175263', {
            target: 'message',
            peerId: 787794248
        }),
        uploaded
    );
});

test('existing private video recovers access_key with the configured user token', async () => {
    const attachments = loadAttachmentsWithMocks({
        axiosGet: async () => {
            throw new Error('video download should not be needed');
        },
        axiosPost: async () => {
            throw new Error('video upload should not be needed');
        },
        vkGet: async (method, params) => {
            if (method === 'video.get') {
                assert.equal(params.videos, '27894453_456');
                assert.equal(params.access_token, 'user-token');
                return {
                    response: {
                        items: [{
                            owner_id: 27894453,
                            id: 456,
                            access_key: 'recovered-video-key'
                        }]
                    }
                };
            }
            throw new Error(`unexpected VK method: ${method}`);
        }
    });

    assert.equal(
        await attachments.processAttachmentWithUserToken(
            'video27894453_456',
            '240175263',
            { target: 'message', peerId: 787794248 }
        ),
        'video27894453_456_recovered-video-key'
    );
});
