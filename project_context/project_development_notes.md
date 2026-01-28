+ we want to analyze cohorts coming from Ethereum to Linea, and there is one contract which governs that, canonical bridge on Ethereum - 0xd19d4B5d358258f05D7B411E21A1460D11B0876F


+ infura endpoints:
    + ethereum-mainnet: https://mainnet.infura.io/v3/{api_key}
    + linea-mainnet: https://linea-mainnet.infura.io/v3/{api_key}

+ tech stack preferred: inura api, python, dbt, postgresql

+ there is around 40k logs to extract from bridge contract, we will use etherscan API for this as Infura API have strict limits regarding number of blocks to query - comparison with etherscan is 1minute and infura is several hours

+ Bridge Contract Address on Ethereum: [0xd19d4B5d358258f05D7B411E21A1460D11B0876F]
+ we are interested to extract only logs with event called MessageSent(address indexed sender, uint256 indexed messageId, uint256 sequenceNumber, bytes message) as they have info about bridged assets - the topic of this event is 0xe856c2b8bd4eb0027ce32eeaf595c21b0b6b4644b326e5b7bd80a1cf8db72e6c
