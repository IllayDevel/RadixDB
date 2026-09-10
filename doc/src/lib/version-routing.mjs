export function chapterDestination(build, locale, chapter) {
  const language = build.locales.includes(locale) ? locale : build.locales[0];
  const preserved = build.chapters.includes(chapter);
  return { href: `${build.base}${language}/${preserved && chapter ? `${chapter}/` : ''}`, preserved };
}

export function validBuild(build) {
  return build && /^\d+\.\d+(?:\.\d+)?$/.test(build.target) &&
    ['release', 'development'].includes(build.channel) &&
    typeof build.base === 'string' && /^\/(?!\/)[A-Za-z0-9_./-]+\/$/.test(build.base) && !build.base.includes('..') &&
    Array.isArray(build.locales) && build.locales.length > 0 && build.locales.every(l => /^[a-z]{2}(?:-[a-z]{2})?$/.test(l)) &&
    Array.isArray(build.chapters) && build.chapters.includes('') && build.chapters.every(p => typeof p === 'string' && (p === '' || /^[a-z0-9_-]+(?:\/[a-z0-9_-]+)*$/.test(p)));
}
