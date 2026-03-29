import fs from 'node:fs/promises';

const [, , inputPath, outputPath, webAclArn] = process.argv;

if (!inputPath || !outputPath || !webAclArn) {
  console.error('Usage: node scripts/update-cloudfront-web-acl.mjs <input.json> <output.json> <web-acl-arn>');
  process.exit(1);
}

const raw = await fs.readFile(inputPath, 'utf8');
const parsed = JSON.parse(raw);
const config = parsed.DistributionConfig ?? parsed;

config.WebACLId = webAclArn;

await fs.writeFile(outputPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
