const test = require('node:test');
const assert = require('node:assert/strict');

const {
    normalizeMiniAppSlug,
    normalizeMiniAppGroupRows,
    listVisibleMiniAppGroups,
    findMiniAppGroupBySlug,
    toDetailDto
} = require('../src/modules/miniapp-groups');

test('normalizeMiniAppSlug trims and lowercases allowed slug characters', () => {
    assert.equal(normalizeMiniAppSlug(' VIP_offer-2026 '), 'vip_offer-2026');
});

test('normalizeMiniAppSlug falls back to the group name', () => {
    assert.equal(normalizeMiniAppSlug('', 'VIP Club'), 'vip-club');
});

test('normalizeMiniAppSlug transliterates basic Russian letters', () => {
    assert.equal(normalizeMiniAppSlug('', 'Привет мир'), 'privet-mir');
});

test('listVisibleMiniAppGroups excludes hidden and disabled groups', () => {
    const groups = normalizeMiniAppGroupRows([
        {
            'Группа': 'Visible',
            'Описание': 'Shown',
            'MiniApp включен': 'да',
            'MiniApp slug': 'visible'
        },
        {
            'Группа': 'Hidden',
            'MiniApp включен': 'да',
            'MiniApp скрыть из списка': 'да',
            'MiniApp slug': 'hidden'
        },
        {
            'Группа': 'Disabled',
            'MiniApp включен': '',
            'MiniApp slug': 'disabled'
        }
    ]);

    assert.deepEqual(listVisibleMiniAppGroups(groups), [
        {
            slug: 'visible',
            title: 'Visible',
            description: 'Shown',
            iconUrl: '',
            subscribed: false
        }
    ]);
});

test('manual icon URL has priority over uploaded icon URL', () => {
    const [group] = normalizeMiniAppGroupRows([
        {
            'Группа': 'VIP',
            'MiniApp включен': 'true',
            'MiniApp иконка URL': 'https://example.com/manual.png',
            'MiniApp иконка файл': 'https://example.com/uploaded.png'
        }
    ]);

    assert.equal(group.iconUrl, 'https://example.com/manual.png');
    assert.equal(toDetailDto(group).iconUrl, 'https://example.com/manual.png');
});

test('findMiniAppGroupBySlug returns hidden enabled group by slug', () => {
    const groups = normalizeMiniAppGroupRows([
        {
            'Группа': 'Secret',
            'MiniApp включен': 'включено',
            'MiniApp скрыть из списка': '1',
            'MiniApp slug': 'secret'
        }
    ]);

    assert.equal(findMiniAppGroupBySlug(groups, ' SECRET ').name, 'Secret');
});

test('findMiniAppGroupBySlug returns null for disabled groups', () => {
    const groups = normalizeMiniAppGroupRows([
        {
            'Группа': 'Disabled',
            'MiniApp включен': '',
            'MiniApp slug': 'disabled'
        }
    ]);

    assert.equal(findMiniAppGroupBySlug(groups, 'disabled'), null);
});

test('normalizeMiniAppGroupRows rejects duplicate enabled slugs', () => {
    assert.throws(
        () => normalizeMiniAppGroupRows([
            {
                'Группа': 'First',
                'MiniApp включен': 'yes',
                'MiniApp slug': 'dup'
            },
            {
                'Группа': 'Second',
                'MiniApp включен': 'вкл',
                'MiniApp slug': 'dup'
            }
        ]),
        /Duplicate MiniApp slug: dup/
    );
});

test('normalizeMiniAppGroupRows allows duplicate disabled slugs and applies defaults', () => {
    const groups = normalizeMiniAppGroupRows([
        {
            'Группа': 'VIP Club',
            'Описание': 'Admin description',
            'MiniApp включен': 'y',
            'MiniApp описание': '',
            'MiniApp баннер URL': '',
            'MiniApp баннер файл': 'https://example.com/banner.png'
        },
        {
            'Группа': 'Disabled One',
            'MiniApp slug': 'dup'
        },
        {
            'Группа': 'Disabled Two',
            'MiniApp slug': 'dup'
        }
    ]);

    assert.equal(groups[0].slug, 'vip-club');
    assert.equal(groups[0].title, 'VIP Club');
    assert.equal(groups[0].description, 'Admin description');
    assert.equal(groups[0].subscribeText, 'Подписаться');
    assert.equal(groups[0].unsubscribeText, 'Отписаться');
    assert.equal(groups[0].bannerUrl, 'https://example.com/banner.png');
});

test('listVisibleMiniAppGroups marks subscribed groups by lowercase group name', () => {
    const groups = normalizeMiniAppGroupRows([
        {
            'Группа': 'VIP Club',
            'MiniApp включен': '1'
        }
    ]);

    assert.equal(listVisibleMiniAppGroups(groups, ['vip club'])[0].subscribed, true);
});
