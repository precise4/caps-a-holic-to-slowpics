import os
import re
import ssl
import certifi
import asyncio
import aiohttp
import aiofiles
from lxml import html
from urllib.parse import urlparse, parse_qs
from requests_toolbelt.multipart.encoder import MultipartEncoder
import requests
import argparse
import urllib.parse
from PIL import Image
import shutil
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from concurrent.futures import ThreadPoolExecutor

def parse_url(url):
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    d1 = query_params.get('d1', [''])[0]
    d2 = query_params.get('d2', [''])[0]
    c = query_params.get('c', [''])[0]
    selected_disks = [d1, d2] if d1 or d2 else []
    return selected_disks, c if c else None


async def disk_info(c: int, image_temp_dir: str):
    url = f'https://caps-a-holic.com/c_list.php?c={c}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            res_content = await response.read()
            tree = html.fromstring(res_content)
            main_title = tree.xpath("//div[@class='big-header']/text()")[0]
            url_str = str(response.url)

            if 'd1=' in url_str:
                d1id, d2id = parse_qs(urlparse(url_str).query).get('d1')[0], parse_qs(urlparse(url_str).query).get('d2')[0]
                info_comp_title = tree.xpath("//div[@class='c-cell' and contains(@style, '400')]")
                d1 = info_comp_title[0].xpath("./text()")
                d2 = info_comp_title[1].xpath("./text()")
                return {
                    d1id: [f"{d1[0]} {d1[1]}", re.findall(r'\d+x\d+', d1[-1])[-1]],
                    d2id: [f"{d2[0]} {d2[1]}", re.findall(r'\d+x\d+', d2[-1])[-1]]
                }, main_title

            disks = tree.xpath("//div[contains(@id, 'd_')]")
            disk_info = {disk.get('id').replace('d_', ''): disk.xpath('.//text()') for disk in disks}
            return disk_info, main_title

async def resolve_images(d1: str, d2: str, c: int):
    images = []
    url = f'https://caps-a-holic.com/c.php?d1={d1}&d2={d2}&c={c}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            res_content = await response.read()
    tree = html.fromstring(res_content)
    image_links = tree.xpath("//a[contains(@href, 'c.php?d1=')]")
    for link in image_links:
        href = link.get('href')
        if href:
            query = urlparse(href).query
            s1 = parse_qs(query).get('s1')
            if s1:
                images.append(s1[0])
    return d1, images

async def gather_images(disk_ids, c: int):
    tasks = [resolve_images(disk_ids[i], disk_ids[i + 1], c) for i in range(len(disk_ids) - 1)]
    if len(disk_ids) > 1:
        tasks.append(resolve_images(disk_ids[-1], disk_ids[0], c))
    results = await asyncio.gather(*tasks)
    return dict(results)

async def fetch_file(id, url, image_temp_dir: str, pbar):
    fname = f'{id}.png'
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    conn = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise Exception(f"Failed to fetch {url}")
            data = await resp.read()
    async with aiofiles.open(os.path.join(image_temp_dir, fname), "wb") as outfile:
        await outfile.write(data)
    pbar.update(1)

async def grab_images(images: dict, height, image_temp_dir: str):
    os.makedirs(image_temp_dir, exist_ok=True)
    total_images = sum(len(img_ids) for img_ids in images.values())
    with tqdm_asyncio(total=total_images, desc="Downloading images") as pbar:
        tasks = [
            fetch_file(img_id, f'https://caps-a-holic.com/c_image.php?max_height={height}&s={img_id}&a=0&y=0&l=1&x=0', image_temp_dir, pbar)
            for img_ids in images.values() for img_id in img_ids
        ]
        await asyncio.gather(*tasks)


async def slowpics_comparison(comp_title, disk_info, image_data, img_dir: str):
    post_data = {
        'collectionName': (None, comp_title),
        'hentai': (None, 'false'),
        'optimizeImages': (None, 'false'),
        'public': (None, 'false')
    }
    z = zip(*image_data.values())
    open_files = []
    for i, item in enumerate(z):
        for j, imgid in enumerate(item):
            disk = disk_info[list(image_data.keys())[j]][0]
            post_data[f'comparisons[{i}].images[{j}].name'] = (None, f"{disk} | {imgid}")
            f = open(os.path.join(img_dir, f"{imgid}.webp"), 'rb')
            open_files.append(f)
            post_data[f'comparisons[{i}].images[{j}].file'] = (f"{imgid}.webp", f, 'image/webp')

    with requests.Session() as client:
        client.get("https://slow.pics/api/comparison")
        files = MultipartEncoder(post_data)
        headers = {
            "Content-Length": str(files.len),
            "Content-Type": files.content_type,
            "X-XSRF-TOKEN": client.cookies.get("XSRF-TOKEN")
        }
        response = client.post("https://slow.pics/api/comparison", data=files, headers=headers)
        print(f'https://slow.pics/c/{response.text}')

    for f in open_files:
        f.close()

async def start_process(selected_disks, c, height, image_temp_dir):
    image_temp_dir = os.path.abspath(image_temp_dir)
    info, main_title = await disk_info(c, image_temp_dir)
    disk_data = []
    if not selected_disks:
        selected_disks = info.keys()
    for d_id in selected_disks:
        d_id = str(d_id)
        if d_id not in info: continue
        disk_data.append(d_id)
        print(info[d_id])
        height = max(height, int(info[d_id][1].rsplit('x', 1)[-1]))
    height = height if height else height
    print(f'Height: {height}')
    print("Gathering Images")
    images = await gather_images(disk_data, c)
    await grab_images(images, height, image_temp_dir)
    await transcode(images, image_temp_dir)
    print("Uploading")
    await slowpics_comparison(main_title, info, images, image_temp_dir)
    shutil.rmtree(image_temp_dir)

async def transcode(images, image_temp_dir):
    def convert_to_webp(img_id):
        infile = os.path.join(image_temp_dir, f'{img_id}.png')
        outfile = os.path.splitext(infile)[0] + '.webp'
        with Image.open(infile).convert("RGB") as im:
            im.save(outfile, "webp", lossless=True)
    with ThreadPoolExecutor() as executor:
        img_ids = [img_id for img_ids in images.values() for img_id in img_ids]
        list(tqdm(executor.map(convert_to_webp, img_ids), total=len(img_ids), desc="Processing images"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload caps-a-holic comparison to slow.pics.')
    parser.add_argument('url', type=str, help='caps-a-holic URL')
    parser.add_argument('--height', type=int, default=0, help='Height override')
    parser.add_argument('-d', '--disks', nargs='*', type=str, help='Additional disks', default=[])
    parser.add_argument('--image_temp_dir', type=str, default='img_tmp', help='Temporary directory for images')
    args = parser.parse_args()
    selected_disks, c = parse_url(args.url)
    height = args.height
    selected_disks.extend(args.disks)
    print(f"Selected disks: {selected_disks}")
    if c is not None:
        print(f"Movie ID: {c}")
    asyncio.run(start_process(selected_disks, c, height, args.image_temp_dir))
