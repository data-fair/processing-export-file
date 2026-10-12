import { spawn, type SpawnOptions } from 'node:child_process'
import Debug from 'debug'

const debug = Debug('spawn-process')

export interface SpawnResult {
  stdout: string
  stderr: string
}

// Run GDAL in a controlled environment, without the secrets and settings of the worker
const ogrEnvKeys = ['PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL', 'TZ', 'LD_LIBRARY_PATH']
const ogrEnvPrefixes = ['GDAL_', 'CPL_', 'OGR_', 'PROJ_']

export const getOgrEnv = (env: NodeJS.ProcessEnv = process.env): Record<string, string> => {
  const ogrEnv: Record<string, string> = {}
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) continue
    if (ogrEnvKeys.includes(key) || ogrEnvPrefixes.some(prefix => key.startsWith(prefix))) ogrEnv[key] = value
  }
  return ogrEnv
}

export async function runCommand (cmd: string, args: string[], options: SpawnOptions = {}): Promise<SpawnResult> {
  debug(`${cmd} ${args.join(' ')}`)
  return await new Promise<SpawnResult>((resolve, reject) => {
    const child = spawn(cmd, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] })
    const stdoutChunks: Buffer[] = []
    const stderrChunks: Buffer[] = []
    child.stdout?.on('data', (chunk: Buffer) => { stdoutChunks.push(chunk) })
    child.stderr?.on('data', (chunk: Buffer) => { stderrChunks.push(chunk) })
    child.on('error', reject)
    child.on('close', (code) => {
      const stdout = Buffer.concat(stdoutChunks).toString()
      const stderr = Buffer.concat(stderrChunks).toString()
      debug('stdout', stdout)
      debug('stderr', stderr)
      if (code === 0) resolve({ stdout, stderr })
      else reject(new Error(stderr || `${cmd} exited with code ${code}`))
    })
  })
}
