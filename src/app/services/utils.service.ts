import { Service } from '@angular/core';

@Service()
export class UtilsService {
    formatHsl(hue: number, saturation: number, light: number): string {
        return `hsl(${hue}, ${saturation * 100}%, ${light * 100}%)`;
    }

    addHue(hue: number, addition: number) {
        const temp = hue + addition;
        return temp - Math.floor(temp / 360) * 360
    }

    hslToRgb(h: number, s: number, l: number) {
        // Normalize inputs to 0-1 ranges
        h /= 360;

        let r, g, b;

        if (s === 0) {
            r = g = b = l; // Achromatic (gray)
        } else {
            const hue2rgb = (p:number, q:number, t:number) => {
            if (t < 0) t += 1;
            if (t > 1) t -= 1;
            if (t < 1/6) return p + (q - p) * 6 * t;
            if (t < 1/2) return q;
            if (t < 2/3) return p + (q - p) * (2/3 - t) * 6;
            return p;
            };

            const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
            const p = 2 * l - q;

            r = hue2rgb(p, q, h + 1/3);
            g = hue2rgb(p, q, h);
            b = hue2rgb(p, q, h - 1/3);
        }

        // Convert back to 0-255 range and round
        return [
            Math.round(r * 255),
            Math.round(g * 255),
            Math.round(b * 255)
        ];
    }

    rgbToHex(red: number, green: number, blue: number) {
        return '#' + [red, green, blue].map(x => x.toString(16).padStart(2, '0')).join('');
    }

    exportJsonAsText(jsonData:any, filename:string = 'data.txt') {
        const jsonString = JSON.stringify(jsonData, null, 2);
        const blob = new Blob([jsonString], { type: 'text/plain;charset=utf-8' });
        const link = document.createElement('a');

        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
        
        URL.revokeObjectURL(link.href);
    }
}
