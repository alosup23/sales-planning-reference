import fs from 'node:fs/promises';

const [, , inputPath, outputPath, albDomainName] = process.argv;

if (!inputPath || !outputPath || !albDomainName) {
  console.error('Usage: node scripts/update-cloudfront-api-origin.mjs <input.json> <output.json> <alb-domain-name>');
  process.exit(1);
}

const raw = await fs.readFile(inputPath, 'utf8');
const parsed = JSON.parse(raw);
const config = parsed.DistributionConfig ?? parsed;

const apiOriginId = 'sales-planning-demo-api-origin';
const apiBehaviorPath = 'api/*';
const healthBehaviorPath = 'health*';

config.Origins ??= { Quantity: 0, Items: [] };
config.CacheBehaviors ??= { Quantity: 0, Items: [] };

const origins = config.Origins.Items ?? [];
const existingOriginIndex = origins.findIndex(origin => origin.Id === apiOriginId);
const apiOrigin = {
  Id: apiOriginId,
  DomainName: albDomainName,
  OriginPath: '',
  CustomHeaders: { Quantity: 0 },
  CustomOriginConfig: {
    HTTPPort: 80,
    HTTPSPort: 443,
    OriginProtocolPolicy: 'http-only',
    OriginSslProtocols: {
      Quantity: 1,
      Items: ['TLSv1.2']
    },
    OriginReadTimeout: 30,
    OriginKeepaliveTimeout: 5
  },
  ConnectionAttempts: 3,
  ConnectionTimeout: 10,
  OriginShield: { Enabled: false },
  OriginAccessControlId: ''
};

if (existingOriginIndex >= 0) {
  origins[existingOriginIndex] = apiOrigin;
} else {
  origins.push(apiOrigin);
}

config.Origins.Items = origins;
config.Origins.Quantity = origins.length;

const baseBehavior = {
  TargetOriginId: apiOriginId,
  TrustedSigners: {
    Enabled: false,
    Quantity: 0
  },
  TrustedKeyGroups: {
    Enabled: false,
    Quantity: 0
  },
  ViewerProtocolPolicy: 'redirect-to-https',
  AllowedMethods: {
    Quantity: 7,
    Items: ['GET', 'HEAD', 'OPTIONS', 'PUT', 'POST', 'PATCH', 'DELETE'],
    CachedMethods: {
      Quantity: 3,
      Items: ['GET', 'HEAD', 'OPTIONS']
    }
  },
  SmoothStreaming: false,
  Compress: true,
  LambdaFunctionAssociations: {
    Quantity: 0
  },
  FunctionAssociations: {
    Quantity: 0
  },
  FieldLevelEncryptionId: '',
  CachePolicyId: '4135ea2d-6df8-44a3-9df3-4b5a84be39ad',
  OriginRequestPolicyId: 'b689b0a8-53d0-40ab-baf2-68738e2966ac',
  GrpcConfig: {
    Enabled: false
  }
};

const behaviors = config.CacheBehaviors.Items ?? [];

function upsertBehavior(pathPattern) {
  const nextBehavior = { ...baseBehavior, PathPattern: pathPattern };
  const index = behaviors.findIndex(behavior => behavior.PathPattern === pathPattern);
  if (index >= 0) {
    behaviors[index] = nextBehavior;
  } else {
    behaviors.push(nextBehavior);
  }
}

upsertBehavior(apiBehaviorPath);
upsertBehavior(healthBehaviorPath);

behaviors.sort((left, right) => left.PathPattern.localeCompare(right.PathPattern, 'en', { sensitivity: 'base' }));

config.CacheBehaviors.Items = behaviors;
config.CacheBehaviors.Quantity = behaviors.length;

await fs.writeFile(outputPath, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
