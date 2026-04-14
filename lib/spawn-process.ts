import { spawn, type SpawnOptions } from 'node:child_process'
import Debug from 'debug'

const debug = Debug('spawn-process')

export interface SpawnResult {
  stdout: string
  stderr: string
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
