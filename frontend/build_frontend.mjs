import * as esbuild from 'esbuild'

await esbuild.build({
  entryPoints: ['src/frontend.js'],
  bundle: true,
  drop:  ["console"],
  outfile: 'build/scripts/frontend.js',
})

await esbuild.build({
    entryPoints: ['src/login.js'],
    bundle: true,
    drop:  ["console"],
    outfile: 'build/scripts/login.js',
  })

  await esbuild.build({
    entryPoints: ['src/lick_archive_base.css'],
    bundle: true,
    outfile: 'build/style/lick_archive_base.css',
  })

  await esbuild.build({
    entryPoints: ['src/dark_theme.css'],
    bundle: true,
    outfile: 'build/style/dark_theme.css',
  })

  await esbuild.build({
    entryPoints: ['src/light_theme.css'],
    bundle: true,
    outfile: 'build/style/light_theme.css',
  })
  