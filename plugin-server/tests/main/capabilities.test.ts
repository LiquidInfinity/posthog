import Piscina from '@posthog/piscina'

import { KafkaQueue } from '../../src/main/ingestion-queues/kafka-queue'
import { startQueues } from '../../src/main/ingestion-queues/queue'
import { startJobsConsumer } from '../../src/main/jobs/job-queue-consumer'
import { Hub, LogLevel } from '../../src/types'
import { createHub } from '../../src/utils/db/hub'

jest.mock('../../src/main/ingestion-queues/kafka-queue')

describe('capabilities', () => {
    let hub: Hub
    let piscina: Piscina
    let closeHub: () => Promise<void>

    beforeEach(async () => {
        ;[hub, closeHub] = await createHub({
            LOG_LEVEL: LogLevel.Warn,
        })
        piscina = { run: jest.fn() } as any
    })

    afterEach(async () => {
        await closeHub()
    })

    describe('queue', () => {
        it('starts ingestion queue by default', async () => {
            const queues = await startQueues(hub, piscina)

            expect(queues).toEqual({
                ingestion: expect.any(KafkaQueue),
            })
        })

        it('handles ingestion being turned off', async () => {
            hub.capabilities.ingestion = false
            hub.capabilities.processAsyncHandlers = false

            const queues = await startQueues(hub, piscina)

            expect(queues).toEqual({
                ingestion: null,
            })
        })
    })

    describe('startJobsConsumer()', () => {
        it('sets up bufferJob handler if ingestion is on', async () => {
            hub.graphileWorker.startConsumer = jest.fn()
            hub.capabilities.ingestion = true
            hub.capabilities.processPluginJobs = false

            await startJobsConsumer(hub, piscina)

            expect(hub.graphileWorker.startConsumer).toHaveBeenCalledWith({
                bufferJob: expect.anything(),
            })
        })

        it('sets up pluginJob handler if processPluginJobs is on', async () => {
            hub.graphileWorker.startConsumer = jest.fn()
            hub.capabilities.ingestion = false
            hub.capabilities.processPluginJobs = true

            await startJobsConsumer(hub, piscina)

            expect(hub.graphileWorker.startConsumer).toHaveBeenCalledWith({
                pluginJob: expect.anything(),
            })
        })

        it('sets up bufferJob and pluginJob handlers if ingestion and processPluginJobs are on', async () => {
            hub.graphileWorker.startConsumer = jest.fn()
            hub.capabilities.ingestion = true
            hub.capabilities.processPluginJobs = true

            await startJobsConsumer(hub, piscina)

            expect(hub.graphileWorker.startConsumer).toHaveBeenCalledWith({
                bufferJob: expect.anything(),
                pluginJob: expect.anything(),
            })
        })
    })
})
